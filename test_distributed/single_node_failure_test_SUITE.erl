%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains test of behaviour in case of node failure and restart without HA.
%%% @end
%%%-------------------------------------------------------------------
-module(single_node_failure_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("distribution_assert.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    failure_test/1
]).

all() -> [
    failure_test
].

-define(ATTEMPTS, 30).
-define(assertInEndedList(Node, SpaceId, ExpectedEndedTransferIds),
    ?assertEqual([], ExpectedEndedTransferIds -- list_ended_transfers(Node, SpaceId), ?ATTEMPTS)
).
-define(assertNotInScheduledList(Node, SpaceId, NotExpectedTransferIds),
    begin
        ScheduledTransfers = list_scheduled_transfers(Node, SpaceId),
        ?assertEqual(ScheduledTransfers, ScheduledTransfers -- NotExpectedTransferIds, ?ATTEMPTS)
    end
).

%%%===================================================================
%%% API
%%%===================================================================

failure_test(Config) ->
    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    oct_environment:disable_panel_healthcheck(Config),

    InitialData = create_initial_data_structure(Config),
    UpdatedConfig = test_base(Config, InitialData, true),
    test_base(UpdatedConfig, InitialData, false),

    ok.

test_base(Config, InitialData, StopAppBeforeKill) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),

    TestData = prepare(InitialData, StopAppBeforeKill),
    ct:pal("Test data prepared"),

    case StopAppBeforeKill of
        true ->
            ?assertEqual(ok, rpc:call(FailingNode, application, stop, [?APP_NAME])),
            ct:pal("Application stopped");
        false ->
            ok
    end,

    failure_test_utils:kill_nodes(Config, FailingNode),
    ct:pal("Node killed"),
    UpdatedConfig = failure_test_utils:restart_nodes(Config, FailingNode),
    oct_background:update_background_config(Config),
    ct:pal("Node restarted"),

    verify( InitialData, TestData),
    ct:pal("Verification finished"),
    UpdatedConfig.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

create_initial_data_structure(Config) ->
    Providers = [P1, P2] = [oct_background:get_provider_id(krakow), oct_background:get_provider_id(paris)],
    [WorkerP1] = oct_background:get_provider_nodes(krakow),
    [WorkerP2] = oct_background:get_provider_nodes(paris),
    SessId = fun(P) ->
        oct_background:get_user_session_id(user1, P)
    end,
    [SpaceId | _] = oct_background:get_provider_supported_spaces(krakow),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    ConflictingDirName = generator:gen_name(),
    {ok, P1DirGuid} =
        ?assertMatch({ok, _}, lfm_proxy:mkdir(WorkerP1, SessId(P1), SpaceGuid, ConflictingDirName, 8#755)),
    {ok, P2DirGuid} =
        ?assertMatch({ok, _}, lfm_proxy:mkdir(WorkerP2, SessId(P2), SpaceGuid, ConflictingDirName, 8#755)),

    lists:foreach(fun(Dir) ->
        lists:foreach(fun({Worker, ProvId}) ->
            ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
                lfm_proxy:stat(Worker, SessId(ProvId), {guid, Dir}), 30)
        end, [{WorkerP1, P1}, {WorkerP2, P2}])
    end, [P1DirGuid, P2DirGuid]),

    FailingProvider = provider_onenv_test_utils:find_importing_provider(Config, SpaceId),
    [HealthyProvider] = Providers -- [FailingProvider],
    #{
        test_dirs => #{
            P1 => P1DirGuid,
            P2 => P2DirGuid
        },
        failing_provider => FailingProvider,
        healthy_provider => HealthyProvider,
        space_id => SpaceId
    }.

prepare(InitialData, StopAppBeforeKill) ->
    TestData = #{stop_app_before_kill => StopAppBeforeKill},
    % take sessions from config because they were updated after restart
    InitialData2 = add_session_ids(InitialData),
    lists:foldl(fun(TestPreparationFunction, TestDataAcc) ->
        TestPreparationFunction(InitialData2, TestDataAcc)
    end, TestData, [
        fun prepare_files/2,
        fun prepare_import/2,
        fun prepare_auto_cleaning/2,
        fun prepare_replication_transfer/2,
        fun prepare_eviction_transfer/2,
        fun prepare_outgoing_migration_transfer/2,
        fun prepare_incoming_migration_transfer/2
    ]).


verify(InitialData, TestData) ->
    % take sessions from config because they were updated after restart
    InitialData2 = add_session_ids(InitialData),
    lists:foreach(fun(TestVerificationFunction) ->
        TestVerificationFunction(InitialData2, TestData)
    end, [
        fun verify_files/2,
        fun verify_import/2,
        fun verify_auto_cleaning/2,
        fun verify_replication_transfer/2,
        fun verify_eviction_transfer/2,
        fun verify_outgoing_migration_transfer/2,
        fun verify_incoming_migration_transfer/2
    ]).



verify_all_files_and_dirs_created_by_provider(Worker, SessId, TestData, ProviderId) ->
    FilesAndDirsMap = kv_utils:get([files_and_dirs, ProviderId], TestData),
    maps:fold(fun(_Key, FilesAndDirs, _) ->
        verify_files_and_dirs(Worker, SessId, FilesAndDirs)
    end, undefined, FilesAndDirsMap).

test_new_files_and_dirs_creation(InitialData, Dir) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [HealthyNode | _] = oct_background:get_provider_nodes(HealthyProvider),

    SessIdFailingProvider = kv_utils:get([session_ids, FailingProvider], InitialData),
    SessIdHealthyProvider = kv_utils:get([session_ids, HealthyProvider], InitialData),

    TestData = create_files_and_dirs(FailingNode, SessIdFailingProvider, Dir),
    verify_files_and_dirs(FailingNode, SessIdFailingProvider, TestData),
    verify_files_and_dirs(HealthyNode, SessIdHealthyProvider, TestData).

create_files_and_dirs(Worker, SessId, ParentGuid) ->
    file_ops_test_utils:create_files_and_dirs(Worker, SessId, ParentGuid, 20, 50).

verify_files_and_dirs(Worker, SessId, DirsAndFiles) ->
    % Verify with 90 attempts as additional time can be needed for
    % connection recreation after node restart
    file_ops_test_utils:verify_files_and_dirs(Worker, SessId, DirsAndFiles, 90).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op"
    }).

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Preparation functions
%%%===================================================================

prepare_files(InitialData, TestData) ->
    [P1, P2] = [oct_background:get_provider_id(krakow), oct_background:get_provider_id(paris)],
    [WorkerP1] = oct_background:get_provider_nodes(krakow),
    [WorkerP2] = oct_background:get_provider_nodes(paris),
    SessId1 = kv_utils:get([session_ids, P1], InitialData),
    SessId2 = kv_utils:get([session_ids, P2], InitialData),
    SpaceId = kv_utils:get(space_id, InitialData),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    P1DirGuid = kv_utils:get([test_dirs, P1], InitialData),
    P2DirGuid = kv_utils:get([test_dirs, P2], InitialData),

    TestData#{
        files_and_dirs => #{
            P1 => #{
                root => create_files_and_dirs(WorkerP1, SessId1, SpaceGuid),
                local => create_files_and_dirs(WorkerP1, SessId1, P1DirGuid),
                remote => create_files_and_dirs(WorkerP1, SessId1, P2DirGuid)
            },
            P2 => #{
                root => create_files_and_dirs(WorkerP2, SessId2, SpaceGuid),
                local => create_files_and_dirs(WorkerP2, SessId2, P2DirGuid),
                remote => create_files_and_dirs(WorkerP2, SessId2, P1DirGuid)
            }
        }
    }.

prepare_import(InitialData, TestData) ->
    ImportingProvider = kv_utils:get(failing_provider, InitialData),
    [ImportingOpNode | _] = oct_background:get_provider_nodes(ImportingProvider),
    SpaceId = kv_utils:get(space_id, InitialData),

    % check whether initial scan has been finished
    storage_import_test_base:assertInitialScanFinished(ImportingOpNode, SpaceId, ?ATTEMPTS),
    storage_import_test_base:assertNoScanInProgress(ImportingOpNode, SpaceId, ?ATTEMPTS),
    FinishedScans = storage_import_test_base:get_finished_scans_num(ImportingOpNode, SpaceId),

    % wait for new files to occur on storage
    % the storage supporting the space is a nulldevice storage with simulated filesystem that grows with the time
    timer:sleep(timer:seconds(1)),
    % mock importing process to block
    block_import(ImportingOpNode),
    % forcefully start import scan
    storage_import_test_base:start_scan(ImportingOpNode, SpaceId),

    TestData#{finished_scans => FinishedScans}.


prepare_auto_cleaning(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    SpaceId = kv_utils:get(space_id, InitialData),
    SessId = kv_utils:get([session_ids, FailingProvider], InitialData),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileSize = 5,

    enable_file_popularity(FailingNode, SpaceId),
    block_auto_cleaning(FailingNode),
    % set high threshold so that auto-cleaning won't start automatically as it may impact other tests
    % set max_file_size = FileSize so that auto-cleaning won't clean bigger files created in other tests
    configure_auto_cleaning(FailingNode, SpaceId, #{
        enabled => true,
        target => 0,
        threshold => 1000000000,
        rules => #{
            enabled => true,
            max_file_size => #{enabled => true, value => FileSize}
        }
    }),
    file_ops_test_utils:create_file(FailingNode, SessId, SpaceGuid, generator:gen_name(), 5),
    ?assertEqual(true, get_current_space_quota(FailingNode, SpaceId) > 0, ?ATTEMPTS),

    {ok, ARId} = force_auto_cleaning_run(FailingNode, SpaceId),
    TestData#{autocleaning_run_id => ARId}.


prepare_replication_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    [HealthyNode | _] = oct_background:get_provider_nodes(HealthyProvider),
    SpaceId = kv_utils:get(space_id, InitialData),
    SessIdFailingProvider = kv_utils:get([session_ids, FailingProvider], InitialData),
    SessIdHealthyProvider = kv_utils:get([session_ids, HealthyProvider], InitialData),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileSize = 10,

    FileGuid = file_ops_test_utils:create_file(HealthyNode, SessIdHealthyProvider, SpaceGuid, generator:gen_name(), FileSize),
    ?assertDistribution(FailingNode, SessIdFailingProvider, ?DISTS([HealthyProvider], [FileSize]), FileGuid, ?ATTEMPTS),

    block_replication_transfer(FailingNode),
    {ok, TransferId} = lfm_proxy:schedule_file_replication(FailingNode, SessIdFailingProvider, {guid, FileGuid}, FailingProvider),
    TestData#{
        replication => #{
            transfer_id => TransferId,
            guid => FileGuid,
            size => FileSize
        }
    }.

prepare_eviction_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    [HealthyNode | _] = oct_background:get_provider_nodes(HealthyProvider),
    SpaceId = kv_utils:get(space_id, InitialData),
    SessIdFailingProvider = kv_utils:get([session_ids, FailingProvider], InitialData),
    SessIdHealthyProvider = kv_utils:get([session_ids, HealthyProvider], InitialData),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileSize = 10,

    FileGuid = file_ops_test_utils:create_file(HealthyNode, SessIdHealthyProvider, SpaceGuid, generator:gen_name(), FileSize),

    % read file to replicate it to FailingProvider
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(FailingNode, SessIdFailingProvider, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertEqual({ok, FileSize}, try
        {ok, Content} = lfm_proxy:check_size_and_read(FailingNode, Handle, 0, 100),
        {ok, byte_size(Content)}
    catch
        _:_ -> error
    end, ?ATTEMPTS),

    block_eviction_transfer(FailingNode),
    {ok, TransferId} =
        lfm_proxy:schedule_file_replica_eviction(FailingNode, SessIdFailingProvider, {guid, FileGuid}, FailingProvider, undefined),
    TestData#{
        eviction => #{
            transfer_id => TransferId,
            guid => FileGuid,
            size => FileSize
        }
    }.

prepare_outgoing_migration_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    [HealthyNode | _] = oct_background:get_provider_nodes(HealthyProvider),
    SpaceId = kv_utils:get(space_id, InitialData),
    SessIdFailingProvider = kv_utils:get([session_ids, FailingProvider], InitialData),
    SessIdHealthyProvider = kv_utils:get([session_ids, HealthyProvider], InitialData),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileSize = 10,

    FileGuid = file_ops_test_utils:create_file(FailingNode, SessIdFailingProvider, SpaceGuid, generator:gen_name(), FileSize),
    ?assertDistribution(HealthyNode, SessIdHealthyProvider, ?DISTS([FailingProvider], [FileSize]), FileGuid, ?ATTEMPTS),

    block_eviction_transfer(FailingNode),
    {ok, TransferId} = lfm_proxy:schedule_file_replica_eviction(FailingNode, SessIdFailingProvider, {guid, FileGuid}, FailingProvider,
        HealthyProvider),

    ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?COMPLETED_STATUS
    }}}, transfer_get(FailingNode, TransferId), 2 * ?ATTEMPTS),

    TestData#{
        outgoing_migration => #{
            transfer_id => TransferId,
            guid => FileGuid,
            size => FileSize
        }
    }.

prepare_incoming_migration_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    [HealthyNode | _] = oct_background:get_provider_nodes(HealthyProvider),
    SpaceId = kv_utils:get(space_id, InitialData),
    SessIdFailingProvider = kv_utils:get([session_ids, FailingProvider], InitialData),
    SessIdHealthyProvider = kv_utils:get([session_ids, HealthyProvider], InitialData),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileSize = 10,

    FileGuid = file_ops_test_utils:create_file(HealthyNode, SessIdHealthyProvider, SpaceGuid, generator:gen_name(), FileSize),
    ?assertDistribution(FailingNode, SessIdFailingProvider, ?DISTS([HealthyProvider], [FileSize]), FileGuid, ?ATTEMPTS),

    block_replication_transfer(FailingNode),
    {ok, TransferId} = lfm_proxy:schedule_file_replica_eviction(FailingNode, SessIdFailingProvider, {guid, FileGuid}, HealthyProvider,
        FailingProvider),

    ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?ENQUEUED_STATUS
    }}}, transfer_get(FailingNode, TransferId), ?ATTEMPTS),

    TestData#{
        incoming_migration => #{
            transfer_id => TransferId,
            guid => FileGuid,
            size => FileSize
        }
    }.

%%%===================================================================
%%% Verification functions
%%%===================================================================

verify_files(InitialData, TestData) ->
    SpaceId = kv_utils:get(space_id, InitialData),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    SessIdFailingProvider = kv_utils:get([session_ids, FailingProvider], InitialData),
    SessIdHealthyProvider = kv_utils:get([session_ids, HealthyProvider], InitialData),
    [FailingNode] =oct_background:get_provider_nodes(FailingProvider),
    [HealthyNode] = oct_background:get_provider_nodes(HealthyProvider),
    verify_all_files_and_dirs_created_by_provider(HealthyNode, SessIdHealthyProvider, TestData, HealthyProvider),
    StopAppBeforeKill = kv_utils:get(stop_app_before_kill, TestData),

    case StopAppBeforeKill of
        true ->
            ?assertEqual(?CLOSING_PROCEDURE_SUCCEEDED, get_application_closing_status(FailingNode)),
            % App was stopped before node killing - all data should be present
            verify_all_files_and_dirs_created_by_provider(FailingNode, SessIdFailingProvider, TestData,
                HealthyProvider),
            verify_all_files_and_dirs_created_by_provider(FailingNode, SessIdFailingProvider, TestData,
                FailingProvider),
            verify_all_files_and_dirs_created_by_provider(HealthyNode, SessIdHealthyProvider, TestData,
                FailingProvider);
        false ->
            ?assertEqual(?CLOSING_PROCEDURE_FAILED, get_application_closing_status(FailingNode)),
            % App wasn't stopped before node killing - some data can be lost
            % but operations on dirs should be possible,
            P1DirGuid = kv_utils:get([test_dirs, FailingProvider], InitialData),
            P2DirGuid = kv_utils:get([test_dirs, HealthyProvider], InitialData),
            test_new_files_and_dirs_creation(InitialData, SpaceGuid),
            test_new_files_and_dirs_creation(InitialData, P1DirGuid),
            test_new_files_and_dirs_creation(InitialData, P2DirGuid)
    end.

verify_import(InitialData, TestData) ->
    ImportingProvider = kv_utils:get(failing_provider, InitialData),
    [ImportingOpNode | _] = oct_background:get_provider_nodes(ImportingProvider),
    SpaceId = kv_utils:get(space_id, InitialData),

    % check whether scan is correctly restarted after node restart
    % get last finished scan
    Scans0 = kv_utils:get(finished_scans, TestData),
    % wait till next scan is finished
    ?assertEqual(Scans0 + 1, storage_import_test_base:get_finished_scans_num(ImportingOpNode, SpaceId), ?ATTEMPTS).


verify_auto_cleaning(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    SpaceId = kv_utils:get(space_id, InitialData),

    ARId1 = kv_utils:get(autocleaning_run_id, TestData),
    % check whether run started before provider was stopped/killed was restarted and finished
    % it will be marked as failed because file was not replicated so it couldn't be cleaned
    ?assertMatch({ok, #{status := <<"failed">>}}, get_auto_cleaning_run_report(FailingNode, ARId1), 2 * ?ATTEMPTS),

    %check whether next run will be started and finished
    % it will be marked as failed because file was not replicated so it couldn't be cleaned
    {ok, ARId2} = force_auto_cleaning_run(FailingNode, SpaceId),
    ?assertMatch({ok, #{status := <<"failed">>}}, get_auto_cleaning_run_report(FailingNode, ARId2), ?ATTEMPTS).


verify_replication_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    SessId = kv_utils:get([session_ids, FailingProvider], InitialData),
    TransferId = kv_utils:get([replication, transfer_id], TestData),
    Guid = kv_utils:get([replication, guid], TestData),
    Size = kv_utils:get([replication, size], TestData),
    SpaceId = kv_utils:get(space_id, InitialData),

    ?assertMatch({ok, #document{value = #transfer{replication_status = ?FAILED_STATUS}}},
        transfer_get(FailingNode, TransferId), ?ATTEMPTS),

    {ok, #document{key = RerunId}} = ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?COMPLETED_STATUS
    }}}, transfer_get_effective(FailingNode, TransferId), ?ATTEMPTS),

    ?assertDistribution(FailingNode, SessId, ?DISTS([FailingProvider, HealthyProvider], [Size, Size]), Guid, ?ATTEMPTS),
    ?assertNotInScheduledList(FailingNode, SpaceId, [TransferId, RerunId]),
    ?assertInEndedList(FailingNode, SpaceId, [TransferId, RerunId]).

verify_eviction_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    SessId = kv_utils:get([session_ids, FailingProvider], InitialData),
    TransferId = kv_utils:get([eviction, transfer_id], TestData),
    Guid = kv_utils:get([eviction, guid], TestData),
    Size = kv_utils:get([eviction, size], TestData),
    SpaceId = kv_utils:get(space_id, InitialData),

    ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?SKIPPED_STATUS,
        eviction_status = ?FAILED_STATUS
    }}}, transfer_get(FailingNode, TransferId), ?ATTEMPTS),

    {ok, #document{key = RerunId}} = ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?SKIPPED_STATUS,
        eviction_status = ?COMPLETED_STATUS
    }}}, transfer_get_effective(FailingNode, TransferId), 2 * ?ATTEMPTS),

    ?assertDistribution(FailingNode, SessId, ?DISTS([FailingProvider, HealthyProvider], [0, Size]), Guid, ?ATTEMPTS),
    ?assertNotInScheduledList(FailingNode, SpaceId, [TransferId, RerunId]),
    ?assertInEndedList(FailingNode, SpaceId, [TransferId, RerunId]).

verify_outgoing_migration_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    SessId = kv_utils:get([session_ids, FailingProvider], InitialData),
    TransferId = kv_utils:get([outgoing_migration, transfer_id], TestData),
    Guid = kv_utils:get([outgoing_migration, guid], TestData),
    Size = kv_utils:get([outgoing_migration, size], TestData),
    SpaceId = kv_utils:get(space_id, InitialData),

    ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?COMPLETED_STATUS,
        eviction_status = ?FAILED_STATUS
    }}}, transfer_get(FailingNode, TransferId), ?ATTEMPTS),

    {ok, #document{key = RerunId}} = ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?COMPLETED_STATUS,
        eviction_status = ?COMPLETED_STATUS
    }}}, transfer_get_effective(FailingNode, TransferId), 2 * ?ATTEMPTS),

    ?assertDistribution(FailingNode, SessId, ?DISTS([FailingProvider, HealthyProvider], [0, Size]), Guid, ?ATTEMPTS),
    ?assertNotInScheduledList(FailingNode, SpaceId, [TransferId, RerunId]),
    ?assertInEndedList(FailingNode, SpaceId, [TransferId, RerunId]).


verify_incoming_migration_transfer(InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    HealthyProvider = kv_utils:get(healthy_provider, InitialData),
    [FailingNode | _] = oct_background:get_provider_nodes(FailingProvider),
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    SessId = kv_utils:get([session_ids, FailingProvider], InitialData),
    TransferId = kv_utils:get([incoming_migration, transfer_id], TestData),
    Guid = kv_utils:get([incoming_migration, guid], TestData),
    Size = kv_utils:get([incoming_migration, size], TestData),
    SpaceId = kv_utils:get(space_id, InitialData),

    ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?FAILED_STATUS,
        eviction_status = ?FAILED_STATUS
    }}}, transfer_get(FailingNode, TransferId), ?ATTEMPTS),

    {ok, #document{key = RerunId}} = ?assertMatch({ok, #document{value = #transfer{
        replication_status = ?COMPLETED_STATUS,
        eviction_status = ?COMPLETED_STATUS
    }}}, transfer_get_effective(FailingNode, TransferId), 2 * ?ATTEMPTS),

    ?assertDistribution(FailingNode, SessId, ?DISTS([HealthyProvider, FailingProvider], [0, Size]), Guid, ?ATTEMPTS),
    ?assertNotInScheduledList(FailingNode, SpaceId, [TransferId, RerunId]),
    ?assertInEndedList(FailingNode, SpaceId, [TransferId, RerunId]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_application_closing_status(Worker) ->
    rpc:call(Worker, datastore_worker, get_application_closing_status, []).


block_import(OpwNode) ->
    test_node_starter:load_modules([OpwNode], [?MODULE]),
    ok = test_utils:mock_new(OpwNode, storage_import_engine),
    ok = test_utils:mock_expect(OpwNode, storage_import_engine, import_file_unsafe, fun(_StorageFileCtx, _Info) ->
        timer:sleep(timer:minutes(10))
    end).


block_auto_cleaning(OpwNode) ->
    test_node_starter:load_modules([OpwNode], [?MODULE]),
    ok = test_utils:mock_new(OpwNode, autocleaning_view_traverse),
    ok = test_utils:mock_expect(OpwNode, autocleaning_view_traverse, process_row, fun(_Row, _Info, _RowNumber) ->
        timer:sleep(timer:minutes(10))
    end).

block_replication_transfer(OpwNode) ->
    test_node_starter:load_modules([OpwNode], [?MODULE]),
    ok = test_utils:mock_new(OpwNode, replication_worker),
    ok = test_utils:mock_expect(OpwNode, replication_worker, transfer_regular_file, fun(_FileCtx, _TransferParams) ->
        timer:sleep(timer:minutes(10))
    end).


block_eviction_transfer(OpwNode) ->
    test_node_starter:load_modules([OpwNode], [?MODULE]),
    ok = test_utils:mock_new(OpwNode, replica_eviction_worker),
    ok = test_utils:mock_expect(OpwNode, replica_eviction_worker, transfer_regular_file,
        fun(_FileCtx, _TransferParams) ->
            timer:sleep(timer:minutes(10))
        end).


get_current_space_quota(Node, SpaceId) ->
    rpc:call(Node, space_quota, current_size, [SpaceId]).

enable_file_popularity(Node, SpaceId) ->
    ok = rpc:call(Node, file_popularity_api, enable, [SpaceId]).

configure_auto_cleaning(Node, SpaceId, Config) ->
    ok = rpc:call(Node, autocleaning_api, configure, [SpaceId, Config]).

force_auto_cleaning_run(Node, SpaceId) ->
    rpc:call(Node, autocleaning_api, force_run, [SpaceId]).

get_auto_cleaning_run_report(Node, ARId) ->
    rpc:call(Node, autocleaning_api, get_run_report, [ARId]).

transfer_get(Node, TransferId) ->
    rpc:call(Node, transfer, get, [TransferId]).

transfer_get_effective(Node, TransferId) ->
    rpc:call(Node, transfer, get_effective, [TransferId]).

list_ended_transfers(Node, SpaceId) ->
    {ok, EndedTransfers} = rpc:call(Node, transfer, list_ended_transfers, [SpaceId]),
    EndedTransfers.

list_scheduled_transfers(Node, SpaceId) ->
    {ok, ScheduledTransfers} = rpc:call(Node, transfer, list_waiting_transfers, [SpaceId]),
    ScheduledTransfers.

add_session_ids(InitialData) ->
    [P1, P2] = [oct_background:get_provider_id(krakow), oct_background:get_provider_id(paris)],
    SessId = fun(P) ->
        oct_background:get_user_session_id(user1, P)
    end,
    InitialData#{
        session_ids => #{
            P1 => SessId(P1),
            P2 => SessId(P2)
        }
    }.