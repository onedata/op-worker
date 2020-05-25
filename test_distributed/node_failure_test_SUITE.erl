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

%%%===================================================================
%%% API
%%%===================================================================

failure_test(Config) ->
    [P1, P2] = test_config:get_providers(Config),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [Worker1P1 | _] = test_config:get_provider_nodes(Config, P1),
    [Worker1P2 | _] = WorkersP2 = test_config:get_provider_nodes(Config, P2),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    Workers = test_config:get_all_op_worker_nodes(Config),
    
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
        {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(WorkerToKillP1, SessId(P1), SpaceGuid, Dir, 8#755)),
        DirGuid
    end, lists:seq(1, 1)),
    Files = lists:map(fun(_) ->
        File = generator:gen_name(),
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(WorkerToKillP1, SessId(P1), SpaceGuid, File, 8#755)),
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(WorkerToKillP1, SessId(P1), {guid, FileGuid}, rdwr)),
        ?assertMatch({ok, _}, lfm_proxy:write(WorkerToKillP1, Handle, 0, FileData)),
        ?assertEqual(ok, lfm_proxy:close(WorkerToKillP1, Handle)),
        FileGuid
    end, lists:seq(1, 1)),
    
    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    ok = onenv_test_utils:disable_panel_healthcheck(Config),
    
    ok = onenv_test_utils:kill_node(Config, WorkerToKillP1),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP1, oneprovider, get_id, []), 10),
    ok = onenv_test_utils:kill_node(Config, WorkerToKillP2),
    ?assertEqual({badrpc, nodedown}, rpc:call(WorkerToKillP2, oneprovider, get_id, []), 10),
    ct:pal("Killed nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),
    
    ok = onenv_test_utils:start_node(Config, WorkerToKillP1),
    ?assertNotEqual({badrpc, nodedown}, rpc:call(WorkerToKillP1, oneprovider, get_id, []), 60),
    ok = onenv_test_utils:start_node(Config, WorkerToKillP2),
    ?assertNotEqual({badrpc, nodedown}, rpc:call(WorkerToKillP2, oneprovider, get_id, []), 60),
    ct:pal("Started nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),
    
%%    lists:foreach(fun(Dir) ->
%%        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
%%            lfm_proxy:stat(WorkerToCheckP2, SessId(P2), {guid, Dir}), Attempts)
%%    end, Dirs),
%%
%%    lists:foreach(fun(Dir) ->
%%        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
%%            lfm_proxy:stat(WorkerToCheckP2, SessId(P2), {guid, Dir}), Attempts)
%%    end, Dirs),
%%
%%    lists:foreach(fun(File) ->
%%        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
%%            lfm_proxy:stat(WorkerToCheckP2, SessId(P2), {guid, File}), Attempts)

    
    % will not work as space is supported by null device storage
%%        ?assertMatch({ok, _, FileData},
%%            begin
%%                {ok, Handle} = lfm_proxy:open(WorkerToCheckP2, SessId(P2), {path, File}, rdwr),
%%                try
%%                    lfm_proxy:read(WorkerToCheckP2, Handle, 0, 1000)
%%                after
%%                    lfm_proxy:close(WorkerToCheckP2, Handle)
%%                end
%%            end, Attempts)
%%    end, Files),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
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
