%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains test of behaviour in case of node failure.
%%% NOTE: Currently it is impossible to fix node after failure during the tests so SUITE should contain single test.
%%% @end
%%%-------------------------------------------------------------------
-module(node_failure_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").

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
    [P1, P2] = test_config:get_providers(Config),
    [Worker1P1 | _] = WorkersP1 = test_config:get_provider_nodes(Config, P1),
    [Worker1P2 | _] = WorkersP2 = test_config:get_provider_nodes(Config, P2),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    Workers = test_config:get_all_op_worker_nodes(Config),
    ClusterManagerNodes = test_config:get_custom(Config, cm_nodes),

    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, ha_datastore, change_config, [2, call])) % TODO VFS-6389 - test with HA cast
    end, Workers),

    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, consistent_hashing, set_nodes_assigned_per_label, [2]))
    end, ClusterManagerNodes),

    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    lists:foreach(fun(PanelNode) ->
        Ctx = rpc:call(PanelNode, service, get_ctx, [op_worker]),
        ok = rpc:call(PanelNode, service, deregister_healthcheck, [op_worker, Ctx])
    end, ?config(op_panel_nodes, Config)),

    timer:sleep(5000), % Give time to flush data saved before HA settings change

    DBsyncWorkerP1 = rpc:call(Worker1P1, datastore_key, responsible_node, [SpaceId]),
    ?assert(is_atom(DBsyncWorkerP1)),
    DBsyncWorkerP2 = rpc:call(Worker1P2, datastore_key, responsible_node, [SpaceId]),
    ?assert(is_atom(DBsyncWorkerP2)),
    GSWorkerP1 = rpc:call(Worker1P1, datastore_key, responsible_node, [?GS_CLIENT_WORKER_GLOBAL_NAME_BIN]),
    ?assert(is_atom(GSWorkerP1)),
    GSWorkerP2 = rpc:call(Worker1P2, datastore_key, responsible_node, [?GS_CLIENT_WORKER_GLOBAL_NAME_BIN]),
    ?assert(is_atom(GSWorkerP2)),

    ct:print("Check dbsync node down:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    ct:print("Check dbsync node down second time:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    [NoDBsyncWorkerP1] = WorkersP1 -- [DBsyncWorkerP1],
    [NoDBsyncWorkerP2] = WorkersP2 -- [DBsyncWorkerP2],

    ct:print("Check second node down:"),
    test_base(Config, NoDBsyncWorkerP1, NoDBsyncWorkerP2),

    ct:print("Check second node down second time:"),
    test_base(Config, NoDBsyncWorkerP1, NoDBsyncWorkerP2),

    ct:print("Check dbsync node down third time:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    ct:print("Check second node down third time:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    ct:print("Check components workers after restarts:"),

    ?assertEqual(DBsyncWorkerP1, node(rpc:call(Worker1P1, global, whereis_name, [{dbsync_out_stream, SpaceId}]))),
    ?assertEqual(DBsyncWorkerP1, node(rpc:call(Worker1P1, global, whereis_name, [{dbsync_in_stream, SpaceId}]))),
    ?assertEqual(DBsyncWorkerP2, node(rpc:call(Worker1P2, global, whereis_name, [{dbsync_out_stream, SpaceId}]))),
    ?assertEqual(DBsyncWorkerP2, node(rpc:call(Worker1P2, global, whereis_name, [{dbsync_in_stream, SpaceId}]))),

    ?assertEqual(GSWorkerP1, rpc:call(Worker1P1, internal_services_manager, get_processing_node,
        [?GS_CLIENT_WORKER_GLOBAL_NAME_BIN])),
    ?assertEqual(GSWorkerP2, rpc:call(Worker1P2, internal_services_manager, get_processing_node,
        [?GS_CLIENT_WORKER_GLOBAL_NAME_BIN])),

    ok.

test_base(Config, WorkerToKillP1, WorkerToKillP2) ->
    [P1, P2] = test_config:get_providers(Config),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [Worker1P1 | _] = WorkersP1 = test_config:get_provider_nodes(Config, P1),
    WorkersP2 = test_config:get_provider_nodes(Config, P2),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = rpc:call(Worker1P1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    Attempts = 30,

    [WorkerToCheckP2] = WorkersP2 -- [WorkerToKillP2],
    [WorkerToCheckP1] = WorkersP1 -- [WorkerToKillP1],

    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    ok = onenv_test_utils:disable_panel_healthcheck(Config),

    ct:print("Init tests using node ~p", [WorkerToKillP1]),
    {Dirs, Files} = create_dirs_and_files(WorkerToKillP1, SpaceGuid, SessId(P1)),

    ok = onenv_test_utils:kill_node(Config, WorkerToKillP1),
    ok = onenv_test_utils:kill_node(Config, WorkerToKillP2),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP1, oneprovider, get_id, []), 10),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP2, oneprovider, get_id, []), 10),
    ct:pal("Killed nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),

    check(WorkerToCheckP2, SessId(P2), Attempts, Dirs, Files),
    ct:print("Check after kill done"),
    timer:sleep(5000),
    {Dirs2, Files2} = create_dirs_and_files(WorkerToCheckP1, SpaceGuid, SessId(P1)),
    ct:print("New dirs and files created"),

    ok = onenv_test_utils:start_node(Config, WorkerToKillP1),
    ok = onenv_test_utils:start_node(Config, WorkerToKillP2),
    ?assertMatch({ok, _}, rpc:call(WorkerToKillP1, provider_auth, get_provider_id, []), 60),
    ?assertMatch({ok, _}, rpc:call(WorkerToKillP2, provider_auth, get_provider_id, []), 60),
    timer:sleep(3000), % TODO - better check node status to be sure that it is running
    ct:pal("Started nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),

    check(WorkerToKillP1, SessId(P1), Attempts, Dirs2, Files2),
    ct:print("Check after restart on P1 done"),
    check(WorkerToKillP2, SessId(P2), Attempts, Dirs2, Files2),
    ct:print("Check after restart on P2 done"),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        Workers = test_config:get_all_op_worker_nodes(Config),
        test_utils:set_env(Workers, ?APP_NAME, session_validity_check_interval_seconds, 1800),
        onenv_test_utils:prepare_base_test_config(NewConfig)
    end,
    test_config:set_many(Config, [
        {add_envs, [op_worker, op_worker, [{key, value}]]},
        {add_envs, [op_worker, cluster_worker, [{key, value}]]},
        {add_envs, [oz_worker, cluster_worker, [{key, value}]]},
        {add_envs, [cluster_manager, cluster_manager, [{key, value}]]},
        {set_onenv_scenario, ["2op-2nodes"]}, % name of yaml file in test_distributed/onenv_scenarios
        {set_posthook, Posthook}
    ]).

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    Config.

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_dirs_and_files(Worker, SpaceGuid, SessId) ->
    Dirs = lists:map(fun(_) ->
        Dir = generator:gen_name(),
        {ok, DirGuid} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, mkdir, [SessId, SpaceGuid, Dir, 8#755], 5000)),
        DirGuid
    end, lists:seq(1, 1)),

    Files = lists:map(fun(_) ->
        File = generator:gen_name(),
        {ok, FileGuid} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, create, [SessId, SpaceGuid, File, 8#755], 5000)),
        {ok, Handle} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, open, [SessId, {guid, FileGuid}, rdwr], 5000)),
        {ok, NewHandle, _} = ?assertMatch({ok, _, _}, rpc:call(Worker, lfm, write,  [Handle, 0, ?FILE_DATA], 5000)),
        ?assertEqual(ok, rpc:call(Worker, lfm, fsync, [NewHandle], 35000)),
        ?assertEqual(ok, rpc:call(Worker, lfm, release, [NewHandle], 5000)),
        FileGuid
    end, lists:seq(1, 1)),

    {Dirs, Files}.

check(Worker, SessId, Attempts, Dirs, Files) ->
    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            rpc:call(Worker, lfm, stat, [SessId, {guid, Dir}], 1000), Attempts)
    end, Dirs),

    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
            rpc:call(Worker, lfm, stat, [SessId, {guid, File}], 1000), Attempts)
    end, Files),

    FileDataSize = size(?FILE_DATA),
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
            end, Attempts)
    end, Files).