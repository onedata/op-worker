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

-define(FILE_DATA, <<"1234567890abcd">>).
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
    [P1, _] = test_config:get_providers(Config),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    ImportingProvider = provider_onenv_test_utils:find_importing_provider(Config, SpaceId),
    [ImportingOpNode | _] = test_config:get_provider_nodes(Config, ImportingProvider),

    TestData = create_test_data(Config, InitialData),
    ct:pal("Test data created"),

    case StopAppBeforeKill of
        true ->
            ?assertEqual(ok, rpc:call(ImportingOpNode, application, stop, [?APP_NAME])),
            ct:pal("Application stopped");
        false ->
            ok
    end,

    failure_test_utils:kill_nodes(Config, ImportingOpNode),
    ct:pal("Node killed"),
    UpdatedConfig = failure_test_utils:restart_nodes(Config, ImportingOpNode),
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

    #{p1_dir => P1DirGuid, p2_dir => P2DirGuid}.

create_test_data(Config, #{p1_dir := P1DirGuid, p2_dir := P2DirGuid}) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    ImportingProvider = provider_onenv_test_utils:find_importing_provider(Config, SpaceId),
    [ImportingOpNode | _] = test_config:get_provider_nodes(Config, ImportingProvider),


    % check whether initial scan has been finished
    storage_import_test_base:assertInitialScanFinished(ImportingOpNode, SpaceId, ?ATTEMPTS),
    storage_import_test_base:assertNoScanInProgress(ImportingOpNode, SpaceId, ?ATTEMPTS),
    FinishedScans = storage_import_test_base:get_finished_scans_num(ImportingOpNode, SpaceId),

    % wait for new files to occur on storage
    timer:sleep(timer:seconds(1)),
    % mock importing process to block
    block_import(ImportingOpNode),
    % forcefully start import scan
    storage_import_test_base:start_scan(ImportingOpNode, SpaceId),

    #{
        p1_root => create_files_and_dirs(WorkerP1, SessId(P1), SpaceGuid),
        p1_local => create_files_and_dirs(WorkerP1, SessId(P1), P1DirGuid),
        p1_remote => create_files_and_dirs(WorkerP1, SessId(P1), P2DirGuid),
        p2_root => create_files_and_dirs(WorkerP2, SessId(P2), SpaceGuid),
        p2_local => create_files_and_dirs(WorkerP2, SessId(P2), P2DirGuid),
        p2_remote => create_files_and_dirs(WorkerP2, SessId(P2), P1DirGuid),
        finished_scans => FinishedScans
    }.

verify(Config, #{p1_dir := P1DirGuid, p2_dir := P2DirGuid} = _InitialData, TestData, StopAppBeforeKill) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    verify_all_files_and_dirs_created_by_provider(WorkerP2, SessId(P2), TestData, p2),

    case StopAppBeforeKill of
        true ->
            % App was stopped before node killing - all data should be present
            verify_all_files_and_dirs_created_by_provider(WorkerP1, SessId(P1), TestData, p2),
            verify_all_files_and_dirs_created_by_provider(WorkerP1, SessId(P1), TestData, p1),
            verify_all_files_and_dirs_created_by_provider(WorkerP2, SessId(P2), TestData, p1);
        false ->
            % App wasn't stopped before node killing - some data can be lost
            % but operations on dirs should be possible,
            test_new_files_and_dirs_creation(Config, SpaceGuid),
            test_new_files_and_dirs_creation(Config, P1DirGuid),
            test_new_files_and_dirs_creation(Config, P2DirGuid)
    end,

    % verify import
    ImportingProvider = provider_onenv_test_utils:find_importing_provider(Config, SpaceId),
    [ImportingOpNode | _] = test_config:get_provider_nodes(Config, ImportingProvider),

    % verify whether further scans is correctly restarted after node restart
    % get last finished scan
    Scans0 = maps:get(finished_scans, TestData),
    % wait till scan is finished
    try

        ?assertEqual(Scans0 + 1, storage_import_test_base:get_finished_scans_num(ImportingOpNode, SpaceId), ?ATTEMPTS)

    catch
        E:R ->
            ct:pal("TEST FAILED: ~p", [{E, R}]),
            ct:timetrap({hours, 10}),
            ct:sleep({hours, 10})
    end.


verify_all_files_and_dirs_created_by_provider(Worker, SessId, TestData, p1) ->
    verify_all_files_and_dirs_created_by_provider(Worker, SessId, TestData,
        [p1_root, p1_local, p1_remote]);
verify_all_files_and_dirs_created_by_provider(Worker, SessId, TestData, p2) ->
    verify_all_files_and_dirs_created_by_provider(Worker, SessId, TestData,
        [p2_root, p2_local, p2_remote]);
verify_all_files_and_dirs_created_by_provider(Worker, SessId, TestData, KeyList) ->
    lists:foreach(fun(Key) ->
        verify_files_and_dirs(Worker, SessId, maps:get(Key, TestData))
    end, KeyList).

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

block_import(OpwNode) ->
    test_node_starter:load_modules([OpwNode], [?MODULE]),
    ok = test_utils:mock_new(OpwNode, storage_import_engine),
    ok = test_utils:mock_expect(OpwNode, storage_import_engine, import_file_unsafe, fun(StorageFileCtx, Info) ->
        timer:sleep(timer:minutes(10))
    end).