%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains tests of tree traverse listing.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse_listing_test_SUITE).
-author("Michal Stanisz").

-include("qos_tests_utils.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    qos_traverse_listing_unexpected_error/1,
    qos_traverse_listing_interrupted_call_error/1,
    archivisation_traverse_listing_unexpected_error/1,
    archivisation_traverse_listing_interrupted_call_error/1,
    archive_verification_traverse_listing_unexpected_error/1,
    archive_verification_traverse_listing_interrupted_call_error/1,
    archive_recall_traverse_listing_unexpected_error/1,
    archive_recall_traverse_listing_interrupted_call_error/1,
    replica_eviction_traverse_listing_unexpected_error/1,
    replica_eviction_traverse_listing_interrupted_call_error/1,
    replication_traverse_listing_unexpected_error/1,
    replication_traverse_listing_interrupted_call_error/1,
    bulk_download_traverse_listing_unexpected_error/1,
    bulk_download_traverse_listing_interrupted_call_error/1,
    tree_deletion_traverse_listing_unexpected_error/1,
    tree_deletion_traverse_listing_interrupted_call_error/1,
    dir_stats_collections_initialization_traverse_listing_unexpected_error/1,
    dir_stats_collections_initialization_traverse_listing_interrupted_call_error/1
]).

all() -> [
    qos_traverse_listing_unexpected_error,
    qos_traverse_listing_interrupted_call_error,
    archivisation_traverse_listing_unexpected_error,
    archivisation_traverse_listing_interrupted_call_error,
    archive_verification_traverse_listing_unexpected_error,
    archive_verification_traverse_listing_interrupted_call_error,
    archive_recall_traverse_listing_unexpected_error,
    archive_recall_traverse_listing_interrupted_call_error,
    replica_eviction_traverse_listing_unexpected_error,
    replica_eviction_traverse_listing_interrupted_call_error,
    replication_traverse_listing_unexpected_error,
    replication_traverse_listing_interrupted_call_error,
    bulk_download_traverse_listing_unexpected_error,
    bulk_download_traverse_listing_interrupted_call_error,
    tree_deletion_traverse_listing_unexpected_error,
    tree_deletion_traverse_listing_interrupted_call_error,
    dir_stats_collections_initialization_traverse_listing_unexpected_error,
    dir_stats_collections_initialization_traverse_listing_interrupted_call_error
].

-define(SPACE_NAME, <<"space1">>).
-define(ATTEMPTS, 10).

-define(UNEXPECTED_ERROR, {error, totally_unexpected_error}).
-define(INTERRUPTED_CALL_ERROR, {error, interrupted_call}).

%%%===================================================================
%%% Tests
%%%===================================================================

qos_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    qos_traverse_listing_error_base().


qos_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    qos_traverse_listing_error_base().


archivisation_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    archivisation_traverse_listing_error_base(unexpected).


archivisation_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    archivisation_traverse_listing_error_base(known).


archive_verification_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    archive_verification_traverse_listing_error_base().


archive_verification_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    archive_verification_traverse_listing_error_base().


archive_recall_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    archive_recall_traverse_listing_error_base().


archive_recall_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    archive_recall_traverse_listing_error_base().


replica_eviction_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    replica_eviction_traverse_listing_error_base(unexpected).


replica_eviction_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    replica_eviction_traverse_listing_error_base(known).


replication_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    replication_traverse_listing_error_base(unexpected).


replication_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    replication_traverse_listing_error_base(known).


bulk_download_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    bulk_download_traverse_listing_error_base(unexpected).


bulk_download_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    bulk_download_traverse_listing_error_base(known).


tree_deletion_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    tree_deletion_traverse_listing_error_base(unexpected).


tree_deletion_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    tree_deletion_traverse_listing_error_base(known).


dir_stats_collections_initialization_traverse_listing_unexpected_error(_Config) ->
    mock_listing(?UNEXPECTED_ERROR),
    dir_stats_collections_initialization_traverse_listing_error_base(unexpected).


dir_stats_collections_initialization_traverse_listing_interrupted_call_error(_Config) ->
    mock_listing(?INTERRUPTED_CALL_ERROR),
    dir_stats_collections_initialization_traverse_listing_error_base(known).

%%%===================================================================
%%% Tests bases
%%%===================================================================

qos_traverse_listing_error_base() ->
    #object{guid = RootDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{}),
    SpaceId = file_id:guid_to_space_id(RootDirGuid),
    FileUuid = file_id:guid_to_uuid(RootDirGuid),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    
    ok = opw_test_rpc:call(krakow, qos_traverse, start, [file_ctx:new_by_guid(RootDirGuid), [<<"dummy_qos_id">>], datastore_key:new()]),
    
    % check that file is added to failed files list only after some time (10s, set in init_per_suite)
    timer:sleep(timer:seconds(8)),
    test_utils:mock_assert_num_calls_sum(KrakowNode, qos_entry, add_to_failed_files_list, [SpaceId, FileUuid], 0),
    test_utils:mock_assert_num_calls_sum(KrakowNode, qos_entry, add_to_failed_files_list, [SpaceId, FileUuid], 1, ?ATTEMPTS),
    test_utils:mock_assert_num_calls_sum(KrakowNode, qos_traverse, task_finished, 2, 1, ?ATTEMPTS),
    
    unmock_listing(),
    
    test_utils:mock_assert_num_calls_sum(KrakowNode, qos_logic, reconcile_qos, [file_ctx:new_by_guid(RootDirGuid)], 1, ?ATTEMPTS).


archivisation_traverse_listing_error_base(ErrorType) ->
    onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{dataset = #dataset_spec{archives = 1}}),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    % archivisation traverse is scheduled with archive creation
    
    case ErrorType of
        unexpected ->
            test_utils:mock_assert_num_calls_sum(KrakowNode, archive, mark_file_failed, 1, 0);
        _ ->
            ok
    end,
    
    test_utils:mock_assert_num_calls_sum(KrakowNode, archivisation_traverse, task_finished, 2, 1, ?ATTEMPTS).


archive_verification_traverse_listing_error_base() ->
    #object{guid = RootDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{}),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    
    ok = opw_test_rpc:call(krakow, archive_verification_traverse, start, [#document{key = datastore_key:new(), value = #archive{data_dir_guid = RootDirGuid}}]),
    
    test_utils:mock_assert_num_calls_sum(KrakowNode, archive, mark_verification_failed, 1, 1),
    test_utils:mock_assert_num_calls_sum(KrakowNode, archive_verification_traverse, task_canceled, 2, 1, ?ATTEMPTS).


archive_recall_traverse_listing_error_base() ->
    #object{guid = RootDirGuid, dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{dataset = #dataset_spec{archives = 1}}),

    {ok, ArchiveDoc} = opw_test_rpc:call(krakow, archive, get, [ArchiveId]),
    UserCtx = opw_test_rpc:call(krakow, user_ctx, new, [<<"0">>]),
    SpaceId = file_id:guid_to_space_id(RootDirGuid),
    
    {ok, _} = opw_test_rpc:call(krakow, archive_recall_traverse, setup_recall_traverse,
        [SpaceId, ArchiveDoc, RootDirGuid, #{
            archive_doc => ArchiveDoc,
            current_parent => fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
            root_file_name => generator:gen_name()
        }, file_ctx:new_by_guid(RootDirGuid), UserCtx]),
    
    check_traverse_retries_until_listing_success(archive_recall_traverse, <<"archive_recall_traverse">>).


replica_eviction_traverse_listing_error_base(ErrorType) ->
    #object{guid = RootDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{}),
    SpaceId = file_id:guid_to_space_id(RootDirGuid),
    FileUuid = file_id:guid_to_uuid(RootDirGuid),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    
    ok = opw_test_rpc:call(krakow, replica_eviction_traverse, start,
        [<<"dummy_provider_id">>, #document{value = #transfer{file_uuid = FileUuid, space_id = SpaceId}}]),
    
    case ErrorType of
        unexpected ->
            check_traverse_retries_until_listing_success(transfer_file_tree_traverse, replica_eviction_traverse:pool_name());
        known ->
            test_utils:mock_assert_num_calls_sum(KrakowNode, transfer_file_tree_traverse, task_finished, 2, 1, ?ATTEMPTS)
    end.


replication_traverse_listing_error_base(ErrorType) ->
    #object{guid = RootDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{}),
    SpaceId = file_id:guid_to_space_id(RootDirGuid),
    FileUuid = file_id:guid_to_uuid(RootDirGuid),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    
    ok = opw_test_rpc:call(krakow, replication_traverse, start,
        [#document{value = #transfer{file_uuid = FileUuid, space_id = SpaceId}}]),
    
    case ErrorType of
        unexpected ->
            check_traverse_retries_until_listing_success(transfer_file_tree_traverse, replica_eviction_traverse:pool_name());
        known ->
            test_utils:mock_assert_num_calls_sum(KrakowNode, transfer_file_tree_traverse, task_finished, 2, 1, ?ATTEMPTS)
    end.


bulk_download_traverse_listing_error_base(ErrorType) ->
    #object{guid = RootDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{}),
    UserCtx = opw_test_rpc:call(krakow, user_ctx, new, [<<"0">>]),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    
    {ok, _} = opw_test_rpc:call(krakow, bulk_download_traverse, start,
        [datastore_key:new(), UserCtx, RootDirGuid, preserve, <<>>]),
    
    case ErrorType of
        unexpected ->
            check_traverse_retries_until_listing_success(bulk_download_traverse, <<"bulk_download_traverse">>);
        known ->
            test_utils:mock_assert_num_calls_sum(KrakowNode, bulk_download_traverse, task_finished, 2, 1, ?ATTEMPTS)
    end.


tree_deletion_traverse_listing_error_base(ErrorType) ->
    #object{guid = RootDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{}),
    SpaceId = file_id:guid_to_space_id(RootDirGuid),
    UserCtx = opw_test_rpc:call(krakow, user_ctx, new, [<<"0">>]),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    
    {ok, _} = opw_test_rpc:call(krakow, tree_deletion_traverse, start,
        [file_ctx:new_by_guid(RootDirGuid), UserCtx, false, fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId), <<>>]),
    
    case ErrorType of
        unexpected ->
            ?assertEqual({ok, 1}, get_number_of_ongoing_traverses(KrakowNode, <<"tree_deletion_traverse">>), ?ATTEMPTS),
            unmock_listing(),
            ?assertEqual({ok, 0}, get_number_of_ongoing_traverses(KrakowNode, <<"tree_deletion_traverse">>), ?ATTEMPTS);
        known ->
            check_traverse_retries_until_listing_success(tree_deletion_traverse, <<"tree_deletion_traverse">>)
    end.


dir_stats_collections_initialization_traverse_listing_error_base(ErrorType) ->
    #object{guid = RootDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, krakow, #dir_spec{}),
    SpaceId = file_id:guid_to_space_id(RootDirGuid),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    
    ok = opw_test_rpc:call(krakow, dir_stats_collections_initialization_traverse, run,
        [SpaceId, global_clock:timestamp_millis()]),
    
    case ErrorType of
        unexpected ->
            test_utils:mock_assert_num_calls_sum(KrakowNode, dir_stats_service_state, disable, 1, 1),
            test_utils:mock_assert_num_calls_sum(KrakowNode, dir_stats_collections_initialization_traverse, task_canceled, 2, 1, ?ATTEMPTS);
        known ->
            check_traverse_retries_until_listing_success(
                dir_stats_collections_initialization_traverse, <<"dir_stats_collections_initialization_traverse">>)
    end.
    

%%%===================================================================
%%% Mocks
%%%===================================================================

mock_listing(Result) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_expect(Nodes, file_listing, list, fun(_, _) -> Result end).

unmock_listing() ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_unload(Nodes, file_listing).

%%%===================================================================
%%% Helper functions
%%%===================================================================

get_number_of_ongoing_traverses(Node, PoolName) ->
    opw_test_rpc:call(Node, fun() ->
        datastore_model:fold_links(
            traverse_task:get_ctx(),
            <<"ONGOING_", PoolName/binary>>, all,
            fun(_, Acc) -> {ok, Acc + 1} end,
            0,
            #{size => 100})
    end).


check_traverse_retries_until_listing_success(TraverseModule, PoolName) ->
    KrakowNode = oct_background:get_random_provider_node(krakow),
    timer:sleep(timer:seconds(8)),
    ?assertEqual({ok, 1}, get_number_of_ongoing_traverses(KrakowNode, PoolName), ?ATTEMPTS),
    test_utils:mock_assert_num_calls_sum(KrakowNode, TraverseModule, task_finished, 2, 0, ?ATTEMPTS),
    unmock_listing(),
    test_utils:mock_assert_num_calls_sum(KrakowNode, TraverseModule, task_finished, 2, 1, ?ATTEMPTS).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, dir_stats_test_utils]} | Config],
        #onenv_test_config{
            onenv_scenario = "1op",
            envs = [{op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60},
                {qos_retry_failed_files_interval_seconds, 2},
                {qos_listing_errors_repeat_timeout_sec, 10},
                {dir_stats_collecting_status_for_new_spaces, disabled}
            ]}],
            posthook = fun(NewConfig) ->
                dir_stats_test_utils:disable_stats_counting(NewConfig),
                NewConfig
            end
        }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(qos_traverse_listing_interrupted_call_error, Config) ->
    qos_traverse_init_per_testcase(Config);
init_per_testcase(qos_traverse_listing_unexpected_error, Config) ->
    qos_traverse_init_per_testcase(Config);
init_per_testcase(archivisation_traverse_listing_interrupted_call_error, Config) ->
    archivisation_traverse_init_per_testcase(Config);
init_per_testcase(archivisation_traverse_listing_unexpected_error, Config) ->
    archivisation_traverse_init_per_testcase(Config);
init_per_testcase(archive_verification_traverse_listing_interrupted_call_error, Config) ->
    archive_verification_traverse_init_per_testcase(Config);
init_per_testcase(archive_verification_traverse_listing_unexpected_error, Config) ->
    archive_verification_traverse_init_per_testcase(Config);
init_per_testcase(archive_recall_traverse_listing_interrupted_call_error, Config) ->
    archive_recall_traverse_init_per_testcase(Config);
init_per_testcase(archive_recall_traverse_listing_unexpected_error, Config) ->
    archive_recall_traverse_init_per_testcase(Config);
init_per_testcase(replica_eviction_traverse_listing_interrupted_call_error, Config) ->
    transfer_traverse_init_per_testcase(Config);
init_per_testcase(replica_eviction_traverse_listing_unexpected_error, Config) ->
    transfer_traverse_init_per_testcase(Config);
init_per_testcase(replication_traverse_listing_interrupted_call_error, Config) ->
    transfer_traverse_init_per_testcase(Config);
init_per_testcase(replication_traverse_listing_unexpected_error, Config) ->
    transfer_traverse_init_per_testcase(Config);
init_per_testcase(bulk_download_traverse_listing_interrupted_call_error, Config) ->
    bulk_download_traverse_init_per_testcase(Config);
init_per_testcase(bulk_download_traverse_listing_unexpected_error, Config) ->
    bulk_download_traverse_init_per_testcase(Config);
init_per_testcase(tree_deletion_traverse_listing_interrupted_call_error, Config) ->
    tree_deletion_traverse_init_per_testcase(Config);
init_per_testcase(tree_deletion_traverse_listing_unexpected_error, Config) ->
    tree_deletion_traverse_init_per_testcase(Config);
init_per_testcase(dir_stats_collections_initialization_traverse_listing_interrupted_call_error, Config) ->
    dir_stats_collections_initialization_traverse_init_per_testcase(Config);
init_per_testcase(dir_stats_collections_initialization_traverse_listing_unexpected_error, Config) ->
    dir_stats_collections_initialization_traverse_init_per_testcase(Config);
init_per_testcase(_, Config) ->
    lfm_proxy:init(Config),
    Config.


end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers),
    lfm_proxy:teardown(Config).


qos_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_listing, [passthrough]),
    test_utils:mock_new(Workers, qos_entry, [passthrough]),
    test_utils:mock_new(Workers, qos_logic, [passthrough]),
    test_utils:mock_new(Workers, qos_traverse, [passthrough]),
    Config.


archivisation_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, archive, [passthrough]),
    test_utils:mock_new(Workers, archivisation_traverse, [passthrough]),
    Config.


archive_verification_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, archive, [passthrough]),
    test_utils:mock_new(Workers, archive_traverses_common, [passthrough]),
    test_utils:mock_expect(Workers, archive_traverses_common, is_cancelling, fun(_) -> false end),
    test_utils:mock_new(Workers, archive_verification_traverse, [passthrough]),
    Config.


archive_recall_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, archive_recall_traverse, [passthrough]),
    Config.


transfer_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, transfer_file_tree_traverse, [passthrough]),
    Config.


bulk_download_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, bulk_download_traverse, [passthrough]),
    Config.


tree_deletion_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, tree_deletion_traverse, [passthrough]),
    test_utils:mock_expect(Workers, tree_deletion_traverse, do_slave_job, fun(_, _) -> ok end),
    Config.


dir_stats_collections_initialization_traverse_init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, dir_stats_collections_initialization_traverse, [passthrough]),
    test_utils:mock_new(Workers, dir_stats_service_state, [passthrough]),
    % mock file_ctx:is_space_dir_const so no additional jobs for archive and trash dirs are created
    test_utils:mock_new(Workers, file_ctx, [passthrough]),
    test_utils:mock_expect(Workers, file_ctx, is_space_dir_const, fun(_) -> false end),
    Config.
