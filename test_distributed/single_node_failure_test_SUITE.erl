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

%%%===================================================================
%%% API
%%%===================================================================

failure_test(Config) ->
    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    % TODO - delete when framework handles node stop properly
    onenv_test_utils:disable_panel_healthcheck(Config),

    InitialData = create_initial_data_structure(Config),
    UpdatedConfig = test_base(Config, InitialData, true),
    test_base(UpdatedConfig, InitialData, false),

    ok.

test_base(Config, InitialData, StopAppBeforeKill) ->
    [P1, _] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),

    TestData = create_test_data(Config, InitialData),
    ct:pal("Test data created"),

    case StopAppBeforeKill of
        true ->
            ?assertEqual(ok, rpc:call(WorkerP1, application, stop, [?APP_NAME])),
            ct:pal("Application stopped");
        false ->
            ok
    end,

    ok = onenv_test_utils:kill_node(Config, WorkerP1),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerP1, oneprovider, get_id, []), 10),
    ct:pal("Node killed"),

    ok = onenv_test_utils:start_node(Config, WorkerP1),
    ?assertMatch({ok, _}, rpc:call(WorkerP1, provider_auth, get_provider_id, []), 60),
    ?assertEqual(true, rpc:call(WorkerP1, gs_channel_service, is_connected, []), 30),
    UpdatedConfig = provider_onenv_test_utils:setup_sessions(proplists:delete(sess_id, Config)),
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
    SpaceGuid = rpc:call(WorkerP1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),

    {ok, P1DirGuid} = ?assertMatch({ok, _}, rpc:call(WorkerP1, lfm, mkdir, [SessId(P1), SpaceGuid, <<"P1_dir">>, 8#755], 5000)),
    {ok, P2DirGuid} = ?assertMatch({ok, _}, rpc:call(WorkerP2, lfm, mkdir, [SessId(P2), SpaceGuid, <<"P1_dir">>, 8#755], 5000)),

    lists:foreach(fun(Dir) ->
        lists:foreach(fun({Worker, ProvId}) ->
            ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
                rpc:call(Worker, lfm, stat, [SessId(ProvId), {guid, Dir}], 1000), 30)
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
    SpaceGuid = rpc:call(WorkerP1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),

    #{
        p1_root => create_files_and_dirs(WorkerP1, SessId(P1), SpaceGuid),
        p1_local => create_files_and_dirs(WorkerP1, SessId(P1), P1DirGuid),
        p1_remote => create_files_and_dirs(WorkerP1, SessId(P1), P2DirGuid),
        p2_root => create_files_and_dirs(WorkerP2, SessId(P2), SpaceGuid),
        p2_local => create_files_and_dirs(WorkerP2, SessId(P2), P2DirGuid),
        p2_remote => create_files_and_dirs(WorkerP2, SessId(P2), P1DirGuid)
    }.

verify(Config, #{p1_dir := P1DirGuid, p2_dir := P2DirGuid} = _InitialData, TestData, StopAppBeforeKill) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = rpc:call(WorkerP1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),

    verify_dir_after_node_restart(WorkerP1, SessId(P1), TestData),
    verify_dir_after_node_restart(WorkerP2, SessId(P2), TestData),

    case StopAppBeforeKill of
        true ->
            % App was stopped before node killing - all data should be present
            verify_files_and_dirs(WorkerP1, SessId(P1), maps:get(p1_root, TestData)),
            verify_files_and_dirs(WorkerP1, SessId(P1), maps:get(p1_local, TestData)),
            verify_files_and_dirs(WorkerP1, SessId(P1), maps:get(p1_remote, TestData));
        false ->
            % App wasn't stopped before node killing - some data can be lost
            % but operations on dirs should be possible,
            test_dir_after_node_restart(Config, SpaceGuid),
            test_dir_after_node_restart(Config, P1DirGuid),
            test_dir_after_node_restart(Config, P2DirGuid)
    end.

verify_dir_after_node_restart(Worker, SessId, TestData) ->
    verify_files_and_dirs(Worker, SessId, maps:get(p2_root, TestData)),
    verify_files_and_dirs(Worker, SessId, maps:get(p2_local, TestData)),
    verify_files_and_dirs(Worker, SessId, maps:get(p2_remote, TestData)).

test_dir_after_node_restart(Config, Dir) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,

    TestData = create_files_and_dirs(WorkerP1, SessId(P1), Dir),
    verify_files_and_dirs(WorkerP1, SessId(P1), TestData),
    verify_files_and_dirs(WorkerP2, SessId(P2), TestData).

create_files_and_dirs(Worker, SessId, ParentUuid) ->
    failure_test_utils:create_files_and_dirs(Worker, SessId, ParentUuid, 20, 50).

verify_files_and_dirs(Worker, SessId, DirsAndFiles) ->
    failure_test_utils:verify_files_and_dirs(Worker, SessId, DirsAndFiles, 30).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    failure_test_utils:init_per_suite(Config, "2op").

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.

end_per_suite(_Config) ->
    ok.