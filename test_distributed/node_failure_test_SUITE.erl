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
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-include("transfers_test_mechanism.hrl").
%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    failure_test/1
]).

all() ->
    ?ALL([failure_test]).

%%%===================================================================
%%% Test functions
%%%===================================================================

failure_test(InitialConfig) ->
    User = <<"user1">>,
    Attempts = 30,
    EnvironmentDescription = {4,0,0,2}, % {SyncNodes, ProxyNodes, ProxyNodesWritten, NodesPerProvider}
    Config = multi_provider_file_ops_test_base:extend_config(InitialConfig, User, EnvironmentDescription, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    SpaceId = ?config(first_space_id, Config),
    FileData = <<"1234567890abcd">>,
    [Worker1P1, _] = ?config(workers1, Config),
    [Worker1P2, _] = WorkersP2 = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),

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
        Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(WorkerToKillP1, SessId(WorkerToKillP1), Dir, 8#755)),
        Dir
    end, lists:seq(1, 1)),
    Files = lists:map(fun(_) ->
        File = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:create(WorkerToKillP1, SessId(WorkerToKillP1), File, 8#755)),
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(WorkerToKillP1, SessId(WorkerToKillP1), {path, File}, rdwr)),
        ?assertMatch({ok, _}, lfm_proxy:write(WorkerToKillP1, Handle, 0, FileData)),
        ?assertEqual(ok, lfm_proxy:close(WorkerToKillP1, Handle)),
        File
    end, lists:seq(1, 1)),

    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP1, erlang, halt, [])),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP2, erlang, halt, [])),

    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            lfm_proxy:stat(WorkerToCheckP2, SessId(WorkerToCheckP2), {path, Dir}), Attempts)
    end, Dirs),

    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            lfm_proxy:stat(WorkerToCheckP2, SessId(WorkerToCheckP2), {path, Dir}), Attempts)
    end, Dirs),

    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
            lfm_proxy:stat(WorkerToCheckP2, SessId(WorkerToCheckP2), {path, File}), Attempts),

        ?assertMatch({ok, FileData},
            begin
                {ok, Handle} = lfm_proxy:open(WorkerToCheckP2, SessId(WorkerToCheckP2), {path, File}, rdwr),
                try
                    lfm_proxy:read(WorkerToCheckP2, Handle, 0, 1000)
                after
                    lfm_proxy:close(WorkerToCheckP2, Handle)
                end
            end, Attempts)
    end, Files),

    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================



%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(_Config) ->
%%    multi_provider_file_ops_test_base:teardown_env(Config). % Do not clean as two nodes were killed
    ok.

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config, false).

end_per_testcase(_Case, _Config) ->
%%    lfm_proxy:teardown(Config). % Do not clean as two nodes were killed
    ok.