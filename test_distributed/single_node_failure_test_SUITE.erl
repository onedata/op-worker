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

    RestartSession = fun() ->
        try
            {ok, provider_onenv_test_utils:setup_sessions(proplists:delete(sess_id, Config))}
        catch
            Error:Reason  ->
                {error, {Error, Reason}}
        end
    end,

    {ok, UpdatedConfig} = ?assertMatch({ok, _}, RestartSession(), 30),
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

create_files_and_dirs(Worker, SessId, ParentUuid) ->
    Dirs = lists:map(fun(_) ->
        Dir = generator:gen_name(),
        {ok, DirGuid} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, mkdir, [SessId, ParentUuid, Dir, 8#755], 5000)),
        DirGuid
    end, lists:seq(1, 20)),

    Files = lists:map(fun(_) ->
        File = generator:gen_name(),
        {ok, FileGuid} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, create, [SessId, ParentUuid, File, 8#755], 5000)),
        {ok, Handle} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, open, [SessId, {guid, FileGuid}, rdwr], 5000)),
        {ok, NewHandle, _} = ?assertMatch({ok, _, _}, rpc:call(Worker, lfm, write,  [Handle, 0, ?FILE_DATA], 5000)),
        ?assertEqual(ok, rpc:call(Worker, lfm, fsync, [NewHandle], 35000)),
        ?assertEqual(ok, rpc:call(Worker, lfm, release, [NewHandle], 5000)),
        FileGuid
    end, lists:seq(1, 50)),

    {Dirs, Files}.

verify_files_and_dirs(Worker, SessId, {Dirs, Files}) ->
    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            rpc:call(Worker, lfm, stat, [SessId, {guid, Dir}], 1000), 30)
    end, Dirs),

    FileDataSize = size(?FILE_DATA),
    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileDataSize}},
            rpc:call(Worker, lfm, stat, [SessId, {guid, File}], 1000), 30)
    end, Files),

    lists:foreach(fun(File) ->
        ?assertEqual(FileDataSize,
            begin
                {ok, Handle} = rpc:call(Worker, lfm, open, [SessId, {guid, File}, rdwr]),
                try
                    {ok, _, ReadData} = rpc:call(Worker, lfm, check_size_and_read, [Handle, 0, 1000]), % use check_size_and_read because of null helper usage
                    size(ReadData) % compare size because of null helper usage
                catch
                    E1:E2 -> {E1, E2}
                after
                    rpc:call(Worker, lfm, release, [Handle])
                end
            end, 30)
    end, Files).

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

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        provider_onenv_test_utils:initialize(NewConfig)
    end,
    test_config:set_many(Config, [
        {set_onenv_scenario, ["2op"]}, % name of yaml file in test_distributed/onenv_scenarios
        {set_posthook, Posthook}
    ]).

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.

end_per_suite(_Config) ->
    ok.