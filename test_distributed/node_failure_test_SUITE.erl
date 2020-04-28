%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(node_failure_test_SUITE).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([all/0]).
-export([init_per_testcase/2]).

-export([
    failure_test/1
]).

all() -> [
    failure_test
].

%%%===================================================================
%%% API
%%%===================================================================

failure_test(Config) ->
    [P1, P2] = kv_utils:get(providers, Config),
    [User1] = kv_utils:get([users, P1], Config),
    SessId = fun(P) -> kv_utils:get([sess_id, P, User1], Config) end,
    [Worker1P1 | _] = kv_utils:get([provider_nodes, P1], Config),
    [Worker1P2 | _] = WorkersP2 = kv_utils:get([provider_nodes, P2], Config),
    [SpaceId | _] = kv_utils:get([provider_spaces, P1], Config),
    Workers = kv_utils:get(op_worker_nodes, Config),
    
    SpaceGuid = rpc:call(Worker1P1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    FileData = <<"1234567890abcd">>,
    Attempts = 30,
    
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, ha_datastore, change_config, [2, call]))
    end, Workers),
    WorkerToKillP1 = rpc:call(Worker1P1, datastore_key, responsible_node, [SpaceId]),
    ?assert(is_atom(WorkerToKillP1)),
    WorkerToKillP2 = rpc:call(Worker1P2, datastore_key, responsible_node, [SpaceId]),
    ?assert(is_atom(WorkerToKillP2)),
    [WorkerToCheckP2] = WorkersP2 -- [WorkerToKillP2],
    
    timer:sleep(5000), % Give time to flush data save before HA settings change
    
    Dirs = lists:map(fun(_) ->
        Dir = generator:gen_name(),
        {ok, DirGuid} = ?assertMatch({ok, _}, rpc:call(WorkerToKillP1, lfm, mkdir, [SessId(P1), SpaceGuid, Dir, 8#755])),
        DirGuid
    end, lists:seq(1, 1)),
    Files = lists:map(fun(_) ->
        File = generator:gen_name(),
        {ok, FileGuid} = ?assertMatch({ok, _}, rpc:call(WorkerToKillP1, lfm, create, [SessId(P1), SpaceGuid, File, 8#755])),
        {ok, Handle} = ?assertMatch({ok, _}, rpc:call(WorkerToKillP1, lfm, open, [SessId(P1), {guid, FileGuid}, rdwr])),
        {ok, NewHandle, _} = ?assertMatch({ok, _, _}, rpc:call(WorkerToKillP1, lfm, write,  [Handle, 0, FileData])),
        ?assertEqual(ok, rpc:call(WorkerToKillP1, lfm, fsync, [NewHandle])),
        ?assertEqual(ok, rpc:call(WorkerToKillP1, lfm, release, [NewHandle])),
        FileGuid
    end, lists:seq(1, 1)),
    
    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    lists:foreach(fun(PanelNode) ->
        Ctx = rpc:call(PanelNode, service, get_ctx, [op_worker]),
        ok = rpc:call(PanelNode, service, deregister_healthcheck, [op_worker, Ctx])
    end, ?config(op_panel_nodes, Config)),
    
    ok = onenv_test_utils:kill_node(Config, WorkerToKillP1, op_worker),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP1, oneprovider, get_id, []), 10),
    ok = onenv_test_utils:kill_node(Config, WorkerToKillP2, op_worker),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP2, oneprovider, get_id, []), 10),
    ct:pal("Killed nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),
    
    ok = onenv_test_utils:start_node(Config, WorkerToKillP1, op_worker),
    ?assertNotEqual({badrpc, nodedown}, rpc:call(WorkerToKillP1, oneprovider, get_id, []), 60),
    ok = onenv_test_utils:start_node(Config, WorkerToKillP2, op_worker),
    ?assertNotEqual({badrpc, nodedown}, rpc:call(WorkerToKillP2, oneprovider, get_id, []), 60),
    ct:pal("Started nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),
    
    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            rpc:call(WorkerToCheckP2, lfm, stat, [SessId(P2), {guid, Dir}]), Attempts)
    end, Dirs),

    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            rpc:call(WorkerToCheckP2, lfm, stat, [SessId(P2), {guid, Dir}]), Attempts)
    end, Dirs),

    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
            rpc:call(WorkerToCheckP2, lfm, stat, [SessId(P2), {guid, File}]), Attempts)

    
    % will not work as space is supported by null device storage
%%        ?assertMatch({ok, _, FileData},
%%            begin
%%                {ok, Handle} = rpc:call(WorkerToCheckP2, lfm, open, [SessId(P2), {guid, File}, rdwr]),
%%                try
%%                    rpc:call(WorkerToCheckP2, lfm, read, [Handle, 0, 1000])
%%                after
%%                    rpc:call(WorkerToCheckP2, lfm, release, [Handle])
%%                end
%%            end, Attempts)
    end, Files),
    
    ok.


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config, false).
