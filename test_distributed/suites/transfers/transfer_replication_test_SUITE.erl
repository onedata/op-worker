%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests replication transfers.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_replication_test_SUITE).
-author("Bartosz Walkowicz").

-include("transfer_test.hrl").
-include("onenv_test_utils.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/storage/helpers/helpers.hrl").
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
    hundred_files_by_view_with_batch_100_test/1,
    hundred_files_by_view_with_batch_10_test/1,
    replication_by_view_emitting_multiple_keys_test/1,

    %% --- no-op replications ---
    replication_to_source_provider_test/1,
    replication_of_already_replicated_file_test/1,
    replication_of_not_synced_file_test/1,

    %% --- space capacity limits ---
    replication_with_exactly_enough_space_test/1,
    replication_into_full_space_test/1,

    %% --- invalid providers ---
    replication_to_missing_provider_test/1,
    replication_to_not_supporting_provider_test/1,
    replication_scheduled_on_not_supporting_provider_test/1,

    %% --- resilience ---
    replication_continues_on_modified_storage_test/1
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
    hundred_files_by_view_with_batch_100_test,
    hundred_files_by_view_with_batch_10_test,
    replication_by_view_emitting_multiple_keys_test,

    %% --- no-op replications ---
    replication_to_source_provider_test,
    replication_of_already_replicated_file_test,
    replication_of_not_synced_file_test,

    %% --- space capacity limits ---
    replication_with_exactly_enough_space_test,
    replication_into_full_space_test,

    %% --- invalid providers ---
    replication_to_missing_provider_test,
    replication_to_not_supporting_provider_test,
    replication_scheduled_on_not_supporting_provider_test,

    %% --- resilience ---
    replication_continues_on_modified_storage_test
].

-define(SUITE_CTX, #transfer_test_suite_ctx{
    transfer_type = replication,
    space_selector = space_krk_par_p,
    user_selector = user1,
    creation_provider_selector = krakow,
    other_provider_selector = paris
}).
-define(run_test(), transfer_common_test_base:?FUNCTION_NAME(?SUITE_CTX)).

-define(MISSING_PROVIDER_ID, <<"missing_provider_id">>).
% space of the "2op" scenario supported only by the creation provider (krakow)
-define(SPACE_NOT_SUPPORTED_BY_OTHER_PROVIDER, space_krk_p).

-define(EXACT_FIT_FILE_SIZE, 100).

% number of files (out of the 10x10 tree) let through the replication jobs
% gate while the modified storage params are in force
-define(FILES_REPLICATED_ON_MODIFIED_STORAGE, 30).


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
hundred_files_by_view_with_batch_100_test(_Config) -> ?run_test().
hundred_files_by_view_with_batch_10_test(_Config) -> ?run_test().


replication_by_view_emitting_multiple_keys_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    #object{children = [FileObject = #object{guid = FileGuid}]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT()}]
        }),

    % the file carries two 'jobId.*' xattrs - the map function emits it
    % under both job id keys
    OtherNode = oct_background:get_random_provider_node(OtherProviderSelector),
    file_test_utils:set_xattr(OtherNode, FileGuid, <<"jobId.1">>, undefined),
    file_test_utils:set_xattr(OtherNode, FileGuid, <<"jobId.2">>, undefined),

    MapFunction = <<
        "function (id, type, meta, ctx) {
            if (type == 'custom_metadata') {
                const JOB_PREFIX = 'jobId.';
                var results = [];
                for (var key of Object.keys(meta)) {
                    if (key.startsWith(JOB_PREFIX)) {
                        var jobId = key.slice(JOB_PREFIX.length);
                        results.push([jobId, id]);
                    }
                }
                return {'list': results};
            }
        }"
    >>,
    ViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),
    transfer_test_utils:create_view(TestSuiteCtx, ViewName, MapFunction, undefined, []),

    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, <<"1">>}], [FileObjectId]
    ),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, <<"2">>}], [FileObjectId]
    ),

    % replication scheduled by one of the keys transfers the file exactly once
    TransferId = transfer_test_utils:schedule_view_transfer(
        TestSuiteCtx, ViewName, [{key, <<"1">>}]
    ),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [FileObject], #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, [FileObject]).


%% --- no-op replications ---


replication_to_source_provider_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        user_selector = UserSelector,
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    #object{children = [FileObject = #object{guid = FileGuid, content = FileContent}]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT()}]
        }),

    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    CreationProviderId = oct_background:get_provider_id(CreationProviderSelector),
    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(
        CreationProviderSelector, SessionId, ?FILE_REF(FileGuid), CreationProviderId
    )),

    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{
        % the source provider already holds the whole file - the transfer
        % completes without replicating anything
        replicating_provider => CreationProviderId,
        files_replicated => 0,
        tree_bytes => 0
    }),

    % the other provider was never involved - its (initially empty)
    % distribution entry must hold no blocks
    transfer_test_utils:await_distribution(
        [CreationProviderSelector, OtherProviderSelector], FileGuid,
        [{CreationProviderSelector, byte_size(FileContent)}, {OtherProviderSelector, 0}]
    ).


replication_of_already_replicated_file_test(_Config) ->
    TestSuiteCtx = ?SUITE_CTX,
    #object{children = [FileObject]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT()}]
        }),

    TransferId1 = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId1, FileObject, #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, FileObject),

    % replicating a file to a provider already holding its full replica
    % completes without transferring anything
    TransferId2 = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId2, FileObject, #{
        files_replicated => 0,
        tree_bytes => 0
    }),
    transfer_test_utils:assert_distribution(TestSuiteCtx, FileObject).


replication_of_not_synced_file_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    #object{children = [FileObject = #object{guid = FileGuid, content = FileContent}]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT()}]
        }),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{
        % the replicating provider does not see the file (its replication
        % traverse errors with not_found) - the transfer completes as a no-op
        files_to_process => 0,
        files_processed => 0,
        files_replicated => 0,
        tree_bytes => 0
    }),

    transfer_test_utils:await_distribution(
        [CreationProviderSelector, OtherProviderSelector], FileGuid,
        [{CreationProviderSelector, byte_size(FileContent)}, {OtherProviderSelector, 0}]
    ).


%% --- space capacity limits ---


replication_with_exactly_enough_space_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        space_selector = SpaceSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    RootDir = transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
        children = [#file_spec{content = ?RAND_CONTENT(?EXACT_FIT_FILE_SIZE)}]
    }),

    SpaceId = oct_background:get_space_id(SpaceSelector),
    SupportSize = transfer_test_utils:get_space_support_size(OtherProviderSelector, SpaceId),
    transfer_test_utils:set_space_occupancy(
        OtherProviderSelector, SpaceId, SupportSize - ?EXACT_FIT_FILE_SIZE
    ),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, RootDir),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, RootDir, #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).


replication_into_full_space_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        space_selector = SpaceSelector,
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    #object{children = [FileObject = #object{guid = FileGuid, content = FileContent}]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT()}]
        }),

    SpaceId = oct_background:get_space_id(SpaceSelector),
    SupportSize = transfer_test_utils:get_space_support_size(OtherProviderSelector, SpaceId),
    transfer_test_utils:set_space_occupancy(OtherProviderSelector, SpaceId, SupportSize),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{
        replication_status => ?FAILED_STATUS,
        failed_files => 1,
        files_replicated => 0,
        tree_bytes => 0
    }),

    % the failed replication must not add any blocks to the target's
    % (initially empty) distribution entry
    transfer_test_utils:await_distribution(
        [CreationProviderSelector, OtherProviderSelector], FileGuid,
        [{CreationProviderSelector, byte_size(FileContent)}, {OtherProviderSelector, 0}]
    ).


%% --- invalid providers ---


replication_to_missing_provider_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        user_selector = UserSelector,
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    #object{children = [#object{guid = FileGuid, content = FileContent}]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT()}]
        }),

    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    ?assertMatch({error, _}, opt_transfers:schedule_file_replication(
        CreationProviderSelector, SessionId, ?FILE_REF(FileGuid), ?MISSING_PROVIDER_ID
    )),

    transfer_test_utils:await_distribution(
        [CreationProviderSelector, OtherProviderSelector], FileGuid,
        [{CreationProviderSelector, byte_size(FileContent)}, {OtherProviderSelector, 0}]
    ).


replication_to_not_supporting_provider_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        user_selector = UserSelector,
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    #object{children = [#object{guid = FileGuid, content = FileContent}]} =
        transfer_test_utils:create_file_tree(
            TestSuiteCtx#transfer_test_suite_ctx{
                space_selector = ?SPACE_NOT_SUPPORTED_BY_OTHER_PROVIDER
            },
            ?FUNCTION_NAME,
            #dir_spec{children = [#file_spec{content = ?RAND_CONTENT()}]}
        ),

    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),
    ?assertMatch({error, _}, opt_transfers:schedule_file_replication(
        CreationProviderSelector, SessionId, ?FILE_REF(FileGuid), OtherProviderId
    )),

    transfer_test_utils:await_distribution(
        [CreationProviderSelector], FileGuid,
        [{CreationProviderSelector, byte_size(FileContent)}]
    ).


replication_scheduled_on_not_supporting_provider_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        user_selector = UserSelector,
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    #object{children = [#object{guid = FileGuid, content = FileContent}]} =
        transfer_test_utils:create_file_tree(
            TestSuiteCtx#transfer_test_suite_ctx{
                space_selector = ?SPACE_NOT_SUPPORTED_BY_OTHER_PROVIDER
            },
            ?FUNCTION_NAME,
            #dir_spec{children = [#file_spec{content = ?RAND_CONTENT()}]}
        ),

    % scheduling on a provider that does not support the space must fail
    SessionId = oct_background:get_user_session_id(UserSelector, OtherProviderSelector),
    CreationProviderId = oct_background:get_provider_id(CreationProviderSelector),
    ?assertMatch({error, _}, opt_transfers:schedule_file_replication(
        OtherProviderSelector, SessionId, ?FILE_REF(FileGuid), CreationProviderId
    )),

    transfer_test_utils:await_distribution(
        [CreationProviderSelector], FileGuid,
        [{CreationProviderSelector, byte_size(FileContent)}]
    ).


%% --- resilience ---


replication_continues_on_modified_storage_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        space_selector = SpaceSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    FileContent = ?RAND_CONTENT(),
    % 10 directories with 10 files each
    RootDir = transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
        children = transfer_test_utils:gen_nested_tree_spec([10, 10], FileContent)
    }),

    % file replication jobs are gated (init_per_testcase) and no permits are
    % granted yet - the scheduled transfer parks on the replicating provider
    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, RootDir),
    transfer_test_utils:await_gated_file_replication_job(),

    % modify storage params while the jobs are parked - the resulting helper
    % reload (and rtransfer restart) must not break the ongoing transfer
    SpaceId = oct_background:get_space_id(SpaceSelector),
    {ok, StorageId} = opw_test_rpc:call(
        OtherProviderSelector, space_logic, get_local_supporting_storage, [SpaceId]
    ),
    Helper = opw_test_rpc:call(OtherProviderSelector, storage, get_helper, [StorageId]),
    HelperArgs = opw_test_rpc:call(OtherProviderSelector, helper, get_args, [Helper]),
    OldTimeout = maps:get(
        <<"timeout">>, HelperArgs, integer_to_binary(?DEFAULT_HELPER_TIMEOUT)
    ),
    ?assertEqual(ok, opw_test_rpc:call(OtherProviderSelector, storage, update_helper_args, [
        StorageId, #{<<"timeout">> => <<"100000">>}
    ])),

    % part of the tree must replicate with the modified params in force
    transfer_test_utils:grant_file_replication_permits(
        OtherProviderSelector, ?FILES_REPLICATED_ON_MODIFIED_STORAGE
    ),
    transfer_test_utils:await_files_replicated(
        OtherProviderSelector, TransferId, ?FILES_REPLICATED_ON_MODIFIED_STORAGE
    ),

    % restore the original params mid-transfer (second helper reload - the
    % remaining files are deterministically still parked) and release the jobs
    ?assertEqual(ok, opw_test_rpc:call(OtherProviderSelector, storage, update_helper_args, [
        StorageId, #{<<"timeout">> => OldTimeout}
    ])),
    transfer_test_utils:grant_file_replication_permits(OtherProviderSelector, all),

    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, RootDir, #{
        % a transfer of this many files may outlast the minute histogram window
        min_hist => skip
    }, ?SCALE_TRANSFER_ATTEMPTS),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, transfer_test_utils, transfer_common_test_base],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case = replication_of_not_synced_file_test, Config) ->
    % simulate scheduling replication of a file the replicating provider has
    % not yet synced - its replication traverse fails to find the file
    #transfer_test_suite_ctx{other_provider_selector = OtherProviderSelector} = ?SUITE_CTX,
    OtherProviderNodes = oct_background:get_provider_nodes(OtherProviderSelector),
    ok = test_utils:mock_new(OtherProviderNodes, tree_traverse),
    ok = test_utils:mock_expect(OtherProviderNodes, tree_traverse, run, fun(_, _, _) ->
        {error, not_found}
    end),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case, Config) when
    Case =:= replication_with_exactly_enough_space_test;
    Case =:= replication_into_full_space_test
->
    #transfer_test_suite_ctx{
        space_selector = SpaceSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    % capture the occupancy only after the default init has removed the
    % leftover file trees of previous runs
    NewConfig = init_per_testcase(?DEFAULT_CASE(Case), Config),
    SpaceId = oct_background:get_space_id(SpaceSelector),
    OccupancyBefore = transfer_test_utils:get_space_occupancy(OtherProviderSelector, SpaceId),
    [{space_occupancy_before, OccupancyBefore} | NewConfig];

init_per_testcase(Case, Config) when
    Case =:= replication_to_not_supporting_provider_test;
    Case =:= replication_scheduled_on_not_supporting_provider_test
->
    % these test cases create their file trees in the not-supported space
    % rather than the suite's one
    TestSuiteCtx = ?SUITE_CTX,
    transfer_test_utils:remove_leftover_file_trees(TestSuiteCtx#transfer_test_suite_ctx{
        space_selector = ?SPACE_NOT_SUPPORTED_BY_OTHER_PROVIDER
    }, Case),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = replication_continues_on_modified_storage_test, Config) ->
    % gate the file replication jobs - the test releases them in stages,
    % interleaving storage modifications with replication progress
    #transfer_test_suite_ctx{other_provider_selector = OtherProviderSelector} = ?SUITE_CTX,
    transfer_test_utils:mock_gated_file_replication(OtherProviderSelector),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    transfer_common_test_base:init_per_testcase(_Case, ?SUITE_CTX, Config).


end_per_testcase(Case = replication_of_not_synced_file_test, Config) ->
    #transfer_test_suite_ctx{other_provider_selector = OtherProviderSelector} = ?SUITE_CTX,
    test_utils:mock_unload(oct_background:get_provider_nodes(OtherProviderSelector), tree_traverse),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= replication_with_exactly_enough_space_test;
    Case =:= replication_into_full_space_test
->
    #transfer_test_suite_ctx{
        space_selector = SpaceSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    SpaceId = oct_background:get_space_id(SpaceSelector),
    OccupancyBefore = ?config(space_occupancy_before, Config),
    transfer_test_utils:set_space_occupancy(OtherProviderSelector, SpaceId, OccupancyBefore),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case = replication_continues_on_modified_storage_test, Config) ->
    #transfer_test_suite_ctx{other_provider_selector = OtherProviderSelector} = ?SUITE_CTX,
    transfer_test_utils:unmock_gated_file_replication(OtherProviderSelector),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    transfer_common_test_base:end_per_testcase(_Case, ?SUITE_CTX, Config).
