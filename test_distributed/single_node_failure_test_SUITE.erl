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
-include_lib("ctool/include/test/test_utils.hrl").

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

-define(FILE_TEST_CONTENT, <<"1234567890abcd">>).
-define(ATTEMPTS, 30).

%%%===================================================================
%%% API
%%%===================================================================

failure_test(Config) ->
    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    onenv_test_utils:disable_panel_healthcheck(Config),

    InitialData = create_initial_data_structure(Config),
    UpdatedConfig = test_base(Config, InitialData, true),
    test_base(UpdatedConfig, InitialData, false),

    ok.

test_base(Config, InitialData, StopAppBeforeKill) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [FailingNode | _] = test_config:get_provider_nodes(Config, FailingProvider),


    TestData = create_test_data(Config, InitialData),
    ct:pal("Test data created"),

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
    ct:pal("Node restarted"),

    verify(UpdatedConfig, InitialData, TestData, StopAppBeforeKill),
    ct:pal("Verification finished"),
    UpdatedConfig.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

create_initial_data_structure(Config) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    {ok, P1DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(WorkerP1, SessId(P1), SpaceGuid, <<"P1_dir">>, 8#755)),
    {ok, P2DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(WorkerP2, SessId(P2), SpaceGuid, <<"P1_dir">>, 8#755)),

    lists:foreach(fun(Dir) ->
        lists:foreach(fun({Worker, ProvId}) ->
            ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
                lfm_proxy:stat(Worker, SessId(ProvId), {guid, Dir}), 30)
        end, [{WorkerP1, P1}, {WorkerP2, P2}])
    end, [P1DirGuid, P2DirGuid]),

    FailingProvider = provider_onenv_test_utils:find_importing_provider(Config, SpaceId),
    #{
        test_dirs => #{
            P1 => P1DirGuid,
            P2 => P2DirGuid
        },
        failing_provider => FailingProvider
    }.

create_test_data(Config, InitialData) ->
    TestData2 = prepare_files(Config, InitialData, #{}),
    TestData3 = prepare_import(Config, InitialData, TestData2),
    prepare_auto_cleaning(Config, InitialData, TestData3).


verify(Config, InitialData, TestData, StopAppBeforeKill) ->
    verify_files(Config, InitialData, TestData, StopAppBeforeKill),
    verify_import(Config, InitialData, TestData),
    verify_auto_cleaning(Config, InitialData, TestData).


verify_all_files_and_dirs_created_by_provider(Worker, SessId, TestData, ProviderId) ->
    FilesAndDirsMap = kv_utils:get([files_and_dirs, ProviderId], TestData),
    maps:fold(fun(_Key, FilesAndDirs, _) ->
        verify_files_and_dirs(Worker, SessId, FilesAndDirs)
    end, undefined, FilesAndDirsMap).

test_new_files_and_dirs_creation(Config, Dir) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,

    TestData = create_files_and_dirs(WorkerP1, SessId(P1), Dir),
    verify_files_and_dirs(WorkerP1, SessId(P1), TestData),
    verify_files_and_dirs(WorkerP2, SessId(P2), TestData).

create_files_and_dirs(Worker, SessId, ParentUuid) ->
    file_ops_test_utils:create_files_and_dirs(Worker, SessId, ParentUuid, 20, 50).

verify_files_and_dirs(Worker, SessId, DirsAndFiles) ->
    % Verify with 90 attempts as additional time can be needed for
    % connection recreation after node restart
    file_ops_test_utils:verify_files_and_dirs(Worker, SessId, DirsAndFiles, 90).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    failure_test_utils:init_per_suite(Config, "2op").

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Preparation functions
%%%===================================================================

prepare_files(Config, InitialData, TestData) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    P1DirGuid = kv_utils:get([test_dirs, P1], InitialData),
    P2DirGuid = kv_utils:get([test_dirs, P2], InitialData),

    TestData#{
        files_and_dirs => #{
            P1 => #{
                root => create_files_and_dirs(WorkerP1, SessId(P1), SpaceGuid),
                local => create_files_and_dirs(WorkerP1, SessId(P1), P1DirGuid),
                remote => create_files_and_dirs(WorkerP1, SessId(P1), P2DirGuid)
            },
            P2 => #{
                root => create_files_and_dirs(WorkerP2, SessId(P2), SpaceGuid),
                local => create_files_and_dirs(WorkerP2, SessId(P2), P2DirGuid),
                remote => create_files_and_dirs(WorkerP2, SessId(P2), P1DirGuid)
            }
        }
    }.

prepare_import(Config, InitialData, TestData) ->
    ImportingProvider = kv_utils:get(failing_provider, InitialData),
    [ImportingOpNode | _] = test_config:get_provider_nodes(Config, ImportingProvider),
    [SpaceId | _] = test_config:get_provider_spaces(Config, ImportingProvider),

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


prepare_auto_cleaning(Config, InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [FailingNode | _] = test_config:get_provider_nodes(Config, FailingProvider),
    [SpaceId | _] = test_config:get_provider_spaces(Config, FailingProvider),
    [User1] = test_config:get_provider_users(Config, FailingProvider),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    ok = rpc:call(FailingNode, file_popularity_api, enable, [SpaceId]),
    block_auto_cleaning(FailingNode),
    ok = rpc:call(FailingNode, autocleaning_api, configure, [SpaceId, #{enabled => true, target => 0}]),

    {ok, {_, Handle}} = lfm_proxy:create_and_open(FailingNode, SessId(FailingProvider), SpaceGuid,
        generator:gen_name(), 8#664),
    {ok, _} = lfm_proxy:write(FailingNode, Handle, 0, ?FILE_TEST_CONTENT),
    lfm_proxy:close(FailingNode, Handle),
    ?assertEqual(true, rpc:call(FailingNode, space_quota, current_size, [SpaceId]) > 0 , ?ATTEMPTS),

    {ok, ARId} = rpc:call(FailingNode, autocleaning_api, force_run, [SpaceId]),

    TestData#{autocleaning_run_id => ARId}.


%%%===================================================================
%%% Verification functions
%%%===================================================================

verify_files(Config, InitialData, TestData, StopAppBeforeKill) ->
    Providers = [P1, P2] = test_config:get_providers(Config),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [NotFailingProvider] = Providers -- [FailingProvider],
    [FailingNode] = test_config:get_provider_nodes(Config, FailingProvider),
    [NotFailingNode] = test_config:get_provider_nodes(Config, NotFailingProvider),
    verify_all_files_and_dirs_created_by_provider(NotFailingNode, SessId(NotFailingProvider), TestData, NotFailingProvider),

    case StopAppBeforeKill of
        true ->
            % App was stopped before node killing - all data should be present
            verify_all_files_and_dirs_created_by_provider(FailingNode, SessId(FailingProvider), TestData, NotFailingProvider),
            verify_all_files_and_dirs_created_by_provider(FailingNode, SessId(FailingProvider), TestData, FailingProvider),
            verify_all_files_and_dirs_created_by_provider(NotFailingNode, SessId(NotFailingProvider), TestData, FailingProvider);
        false ->
            % App wasn't stopped before node killing - some data can be lost
            % but operations on dirs should be possible,
            P1DirGuid = kv_utils:get([test_dirs, P1], InitialData),
            P2DirGuid = kv_utils:get([test_dirs, P2], InitialData),
            test_new_files_and_dirs_creation(Config, SpaceGuid),
            test_new_files_and_dirs_creation(Config, P1DirGuid),
            test_new_files_and_dirs_creation(Config, P2DirGuid)
    end.

verify_import(Config, InitialData, TestData) ->
    ImportingProvider = kv_utils:get(failing_provider, InitialData),
    [ImportingOpNode | _] = test_config:get_provider_nodes(Config, ImportingProvider),
    [SpaceId | _] = test_config:get_provider_spaces(Config, ImportingProvider),

    % check whether scan is correctly restarted after node restart
    % get last finished scan
    Scans0 = kv_utils:get(finished_scans, TestData),
    % wait till next scan is finished
    ?assertEqual(Scans0 + 1, storage_import_test_base:get_finished_scans_num(ImportingOpNode, SpaceId), ?ATTEMPTS).


verify_auto_cleaning(Config, InitialData, TestData) ->
    FailingProvider = kv_utils:get(failing_provider, InitialData),
    [FailingNode | _] = test_config:get_provider_nodes(Config, FailingProvider),
    [SpaceId | _] = test_config:get_provider_spaces(Config, FailingProvider),

    % auto-cleaning runs will be failed as cleanup was not performed because there was nothing to clean

    ARId1 = kv_utils:get(autocleaning_run_id, TestData),
    %check whether run started before provider was stopped/killed was restarted and finished
    ?assertMatch({ok, #{status := <<"failed">>}}, rpc:call(FailingNode, autocleaning_api, get_run_report, [ARId1]), ?ATTEMPTS),

    %check whether next run will be started and finished
    {ok, ARId2} = rpc:call(FailingNode, autocleaning_api, force_run, [SpaceId]),
    ?assertMatch({ok, #{status := <<"failed">>}}, rpc:call(FailingNode, autocleaning_api, get_run_report, [ARId2]), ?ATTEMPTS).



%%%===================================================================
%%% Internal functions
%%%===================================================================

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