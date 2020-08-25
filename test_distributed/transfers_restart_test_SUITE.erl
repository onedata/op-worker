%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains test of transfers in case of node restart without HA.
%%% @end
%%%-------------------------------------------------------------------
-module(transfers_restart_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    rtransfer_restart_test/1,
    node_restart_test/1,
    rtransfer_restart_test2/1,
    node_restart_test2/1
]).

all() -> [
    rtransfer_restart_test,
    node_restart_test
    % TODO - delete further tests
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2,
%%    rtransfer_restart_test2,
%%    node_restart_test2
].

-define(FILE_DATA, <<"1234567890abcd">>).

%%%===================================================================
%%% API
%%%===================================================================

rtransfer_restart_test(Config) ->
    RestartFun = fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, rtransfer_config, restart_link, [])),
        ct:pal("Rtransfer restarted")
    end,

    restart_test_base(Config, RestartFun, false).

% TODO - delete fun
rtransfer_restart_test2(Config) ->
    NewConfig = provider_onenv_test_utils:initialize(Config),
    rtransfer_restart_test(NewConfig).

node_restart_test(Config) ->
    RestartFun = fun(Worker) ->
        ok = onenv_test_utils:kill_node(Config, Worker),
        ?assertEqual({badrpc, nodedown}, rpc:call(Worker, oneprovider, get_id, []), 10),
        ct:pal("Node killed"),

        ok = onenv_test_utils:start_node(Config, Worker),
        ?assertMatch({ok, _}, rpc:call(Worker, provider_auth, get_provider_id, []), 60),
        ct:pal("Node restarted")
    end,

    restart_test_base(Config, RestartFun, true).

% TODO - delete fun
node_restart_test2(Config) ->
    NewConfig = provider_onenv_test_utils:initialize(Config),
    node_restart_test(NewConfig).

restart_test_base(Config, RestartFun, NodeRestart) ->
    [P1, P2] = test_config:get_providers(Config),
    [WorkerP1] = test_config:get_provider_nodes(Config, P1),
    [WorkerP2] = test_config:get_provider_nodes(Config, P2),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = rpc:call(WorkerP1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    SessIdP1 = SessId(P1),
    SessIdP2 = SessId(P2),
    Attempts = 120,
    FileSize = byte_size(?FILE_DATA),
    Priority = 32,
    UserCtxP2 = rpc:call(WorkerP2, user_ctx, new, [SessIdP2]),

    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    % TODO - delete when framework handles node stop properly
    onenv_test_utils:disable_panel_healthcheck(Config),

    Files1 = lists:map(fun(_) ->
        create_file(WorkerP1, SessIdP1, SpaceGuid)
    end, lists:seq(1, 1000)),
    Files2 = lists:map(fun(_) ->
        create_file(WorkerP1, SessIdP1, SpaceGuid)
    end, lists:seq(1, 1000)),
    AllFiles = Files1 ++ Files2,

    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileSize}},
            rpc:call(WorkerP2, lfm, stat, [SessIdP2, {guid, File}]), Attempts)
    end, AllFiles),

    lists_utils:pforeach(fun(File) ->
        FileCtx = file_ctx:new_by_guid(File),
        lists:foreach(fun(Offset) ->
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                rpc:call(WorkerP2, sync_req, request_block_synchronization,
                    [UserCtxP2, FileCtx, #file_block{offset = Offset, size = 1}, false, undefined, Priority]))
        end, lists:seq(0, FileSize))
    end, Files1),

    PMapAns = lists_utils:pmap(fun(File) ->
        FileCtx = file_ctx:new_by_guid(File),
        lists:foreach(fun(Offset) ->
            ?assertMatch(#fuse_response{status = #status{code = ?OK}},
                rpc:call(WorkerP2, sync_req, request_block_synchronization,
                    [UserCtxP2, FileCtx, #file_block{offset = Offset, size = 1}, false, undefined, Priority]))
        end, lists:seq(0,9)),
        rpc:call(WorkerP2, lfm, schedule_file_replication, [SessIdP2, {guid, File}, P2, undefined])
    end, Files2),

    TransferIDs = lists:map(fun(Ans) ->
        {ok, TransferID} = ?assertMatch({ok, _}, Ans),
        TransferID
    end, PMapAns),

    RestartFun(WorkerP2),

    lists:foreach(fun(TransferId) ->
        multi_provider_file_ops_test_base:await_replication_end(WorkerP2, TransferId, Attempts, get_effective)
    end, TransferIDs),

    FilesToCheckDistribution = case NodeRestart of
        true -> Files2;
        false -> AllFiles
    end,
    lists:foreach(fun(File) ->
        % Use root session as user session is not valid after restart
        ?assertMatch({ok, [
            #{<<"blocks">> := [[0, FileSize]], <<"totalBlocksSize">> := FileSize},
            #{<<"blocks">> := [[0, FileSize]], <<"totalBlocksSize">> := FileSize}
        ]}, rpc:call(WorkerP2, lfm, get_file_distribution, [?ROOT_SESS_ID, {guid, File}]), Attempts)
    end, FilesToCheckDistribution),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        % TODO - delete setting envs
        Workers = test_config:get_all_op_worker_nodes(Config),
        test_utils:set_env(Workers, ?APP_NAME, session_validity_check_interval_seconds, 1800),
        test_utils:set_env(Workers, ?APP_NAME, fuse_session_grace_period_seconds, 1800),
        provider_onenv_test_utils:initialize(NewConfig)
    end,
    test_config:set_many(Config, [
        {add_envs, [op_worker, op_worker, [{key, value}]]},
        {add_envs, [op_worker, cluster_worker, [{key, value}]]},
        {add_envs, [oz_worker, cluster_worker, [{key, value}]]},
        {add_envs, [cluster_manager, cluster_manager, [{key, value}]]},
        {set_onenv_scenario, ["2op"]}, % name of yaml file in test_distributed/onenv_scenarios
        {set_posthook, Posthook}
    ]).

init_per_testcase(_Case, Config) ->
    Workers = test_config:get_all_op_worker_nodes(Config),
    test_utils:set_env(Workers, ?APP_NAME, minimal_sync_request, 1),
    test_utils:set_env(Workers, ?APP_NAME, synchronizer_block_suiting, false),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.

end_per_suite(_Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_file(Worker, SessId, SpaceGuid) ->
    {ok, FileGuid} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, create,
        [SessId, SpaceGuid, generator:gen_name(), 8#755], 5000)),
    {ok, Handle} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, open, [SessId, {guid, FileGuid}, rdwr], 5000)),
    {ok, NewHandle, _} = ?assertMatch({ok, _, _}, rpc:call(Worker, lfm, write,  [Handle, 0, ?FILE_DATA], 5000)),
    ?assertEqual(ok, rpc:call(Worker, lfm, fsync, [NewHandle], 35000)),
    ?assertEqual(ok, rpc:call(Worker, lfm, release, [NewHandle], 5000)),
    FileGuid.