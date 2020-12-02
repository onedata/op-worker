%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains test of behaviour in case of node failure.
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

%%%===================================================================
%%% API
%%%===================================================================

failure_test(Config) ->
    [P1, P2] = test_config:get_providers(Config),
    [Worker1P1 | _] = WorkersP1 = test_config:get_provider_nodes(Config, P1),
    [Worker1P2 | _] = WorkersP2 = test_config:get_provider_nodes(Config, P2),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),

    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    ok = onenv_test_utils:disable_panel_healthcheck(Config),

    enable_ha(Config),

    % Get services nodes to check if everything returns on proper node after all nodes failures
    DBsyncWorkerP1 = responsible_node(Worker1P1, SpaceId),
    DBsyncWorkerP2 = responsible_node(Worker1P2, SpaceId),
    GSWorkerP1 = responsible_node(Worker1P1, ?GS_CHANNEL_SERVICE_NAME),
    GSWorkerP2 = responsible_node(Worker1P2, ?GS_CHANNEL_SERVICE_NAME),

    % Execute base test that kill nodes many times to verify if multiple failures will be handled properly
    ct:pal("Check dbsync node down:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    ct:pal("Check dbsync node down second time:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    [NoDBsyncWorkerP1] = WorkersP1 -- [DBsyncWorkerP1],
    [NoDBsyncWorkerP2] = WorkersP2 -- [DBsyncWorkerP2],

    ct:pal("Check second node down:"),
    test_base(Config, NoDBsyncWorkerP1, NoDBsyncWorkerP2),

    ct:pal("Check second node down second time:"),
    test_base(Config, NoDBsyncWorkerP1, NoDBsyncWorkerP2),

    ct:pal("Check dbsync node down third time:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    ct:pal("Check second node down third time:"),
    test_base(Config, DBsyncWorkerP1, DBsyncWorkerP2),

    % Verify if everything returns on proper node after all nodes failures
    ct:pal("Check components workers after restarts:"),
    verify_dbsync_stream_node(DBsyncWorkerP1, Worker1P1, dbsync_out_stream, SpaceId),
    verify_dbsync_stream_node(DBsyncWorkerP1, Worker1P1, dbsync_in_stream, SpaceId),
    verify_dbsync_stream_node(DBsyncWorkerP2, Worker1P2, dbsync_out_stream, SpaceId),
    verify_dbsync_stream_node(DBsyncWorkerP2, Worker1P2, dbsync_in_stream, SpaceId),

    verify_gs_channel_node(GSWorkerP1, Worker1P1),
    verify_gs_channel_node(GSWorkerP2, Worker1P2),

    ok.

% Basic test - creates dirs and files and checks if they can be used after node failure
% Creates also dirs and files when node is down and checks if they can be used after node recovery
test_base(Config, WorkerToKillP1, WorkerToKillP2) ->
    [OzNode | _ ] = test_config:get_all_oz_worker_nodes(Config),
    [P1, P2] = test_config:get_providers(Config),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    WorkersP1 = test_config:get_provider_nodes(Config, P1),
    WorkersP2 = test_config:get_provider_nodes(Config, P2),
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Attempts = 60,

    [WorkerToCheckP2] = WorkersP2 -- [WorkerToKillP2],
    [WorkerToCheckP1] = WorkersP1 -- [WorkerToKillP1],

    % TODO VFS-7037 uncomment and make it pass
%%    ct:pal("Create fuse sessions on nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),
%%    AccessToken = provider_onenv_test_utils:create_oz_temp_access_token(OzNode, User1),
%%    {FuseSessIdP1, ConnectionsP1} = setup_fuse_session_with_connections(
%%        AccessToken, WorkerToKillP1, WorkerToCheckP1, SpaceGuid, Attempts
%%    ),
%%    {FuseSessIdP2, ConnectionsP2} = setup_fuse_session_with_connections(
%%        AccessToken, WorkerToKillP2, WorkerToCheckP2, SpaceGuid, Attempts
%%    ),

    ct:pal("Init tests using node ~p", [WorkerToKillP1]),
    DirsAndFiles = create_dirs_and_files(WorkerToKillP1, SessId(P1), SpaceGuid),

    failure_test_utils:kill_nodes(Config, [WorkerToKillP1, WorkerToKillP2]),
    ct:pal("Killed nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),

    file_ops_test_utils:verify_files_and_dirs(WorkerToCheckP2, SessId(P2), DirsAndFiles, Attempts),
    ct:pal("Files check after node kill: done"),
    timer:sleep(5000),
    DirsAndFiles2 = create_dirs_and_files(WorkerToCheckP1, SessId(P1), SpaceGuid),
    ct:pal("New dirs and files created"),

    _UpdatedConfig = failure_test_utils:restart_nodes(Config, [WorkerToKillP1, WorkerToKillP2]),
    ct:pal("Started nodes: ~n~p~n~p", [WorkerToKillP1, WorkerToKillP2]),

    file_ops_test_utils:verify_files_and_dirs(WorkerToKillP1, SessId(P1), DirsAndFiles2, Attempts),
    ct:pal("Files check on P1 after restart: done"),
    file_ops_test_utils:verify_files_and_dirs(WorkerToKillP2, SessId(P2), DirsAndFiles2, Attempts),
    ct:pal("Files check on P2 after restart: done"),

    % TODO VFS-7037 uncomment and make it pass
%%    verify_fuse_session_after_restart(
%%        FuseSessIdP1, ConnectionsP1, WorkerToKillP1, WorkerToCheckP1, SpaceGuid, Attempts
%%    ),
%%    ct:pal("Fuse session check on P1 after restart: done"),
%%    verify_fuse_session_after_restart(
%%        FuseSessIdP2, ConnectionsP2, WorkerToKillP2, WorkerToCheckP2, SpaceGuid, Attempts
%%    ),
%%    ct:pal("Fuse session check on P2 after restart: done"),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    failure_test_utils:init_per_suite(Config, "2op-2nodes").

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

enable_ha(Config) ->
    [P1, P2] = test_config:get_providers(Config),
    Workers = test_config:get_all_op_worker_nodes(Config),
    CM_P1 = test_config:get_custom(Config, [primary_cm, P1]),
    CM_P2 = test_config:get_custom(Config, [primary_cm, P2]),
    ClusterManagerNodes = [CM_P1, CM_P2],

    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, ha_datastore, change_config, [2, call])) % TODO VFS-6389 - test with HA cast
    end, Workers),

    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, consistent_hashing, set_nodes_assigned_per_label, [2]))
    end, ClusterManagerNodes),

    timer:sleep(10000). % Give time to flush data saved before HA settings change

create_dirs_and_files(Worker, SessId, SpaceGuid) ->
    file_ops_test_utils:create_files_and_dirs(Worker, SessId, SpaceGuid, 1, 1).

responsible_node(NodeToCall, TermToCheck) ->
    Node = rpc:call(NodeToCall, datastore_key, any_responsible_node, [TermToCheck]),
    ?assert(is_atom(Node)),
    Node.

verify_dbsync_stream_node(ExpectedWorker, NodeToCall, StreamType, SpaceId) ->
    ?assertEqual(ExpectedWorker, node(rpc:call(NodeToCall, global, whereis_name, [{StreamType, SpaceId}]))).

verify_gs_channel_node(ExpectedWorker, NodeToCall) ->
    ?assertEqual(ExpectedWorker, rpc:call(NodeToCall, internal_services_manager, get_processing_node,
        [?GS_CHANNEL_SERVICE_NAME])).

setup_fuse_session_with_connections(AccessToken, WorkerToKill, WorkerToCheck, SpaceGuid, Attempts) ->
    Nonce = crypto:strong_rand_bytes(10),

    Opts = [{active, true}],
    {ok, {Sock1, SessId}} = fuse_test_utils:connect_via_token(WorkerToKill, Opts, Nonce, AccessToken),
    {ok, {Sock2, SessId}} = fuse_test_utils:connect_via_token(WorkerToCheck, Opts, Nonce, AccessToken),

    {ok, #document{
        value = #session{
            supervisor = Sup,
            event_manager = EventManager,
            watcher = SessionWatcher,
            sequencer_manager = SeqManager,
            async_request_manager = AsyncReqManager,
            connections = [_ | _] = Cons
        }}
    } = ?assertMatch(
        {ok, #document{value = #session{node = WorkerToKill, connections = [_, _]}}},
        rpc:call(WorkerToKill, session, get, [SessId]),
        Attempts
    ),

    % Assert that all session processes were created on WorkerToKill with exception to
    % connections which should be on each node.
    assert_processes_reside_on_node(WorkerToKill, [
        Sup, EventManager, SessionWatcher, SeqManager, AsyncReqManager
    ]),

    ExpConnectionNodes = lists:sort([WorkerToKill, WorkerToCheck]),
    ?assertEqual(
        ExpConnectionNodes,
        lists:usort(lists:map(fun(Connection) -> node(Connection) end, Cons))
    ),

    assert_proper_connections_rib(SessId, AsyncReqManager, Cons),

    % Assert all working connections
    lists:foreach(fun(Sock) -> fuse_test_utils:ls(Sock, SpaceGuid) end, [Sock1, Sock2]),

    {SessId, [{WorkerToKill, Sock1}, {WorkerToCheck, Sock2}]}.

verify_fuse_session_after_restart(SessId, ConnectionPerWorker, WorkerToKill, WorkerToCheck, SpaceGuid, Attempts) ->
    {ok, #document{
        value = #session{
            supervisor = Sup,
            event_manager = EventManager,
            watcher = SessionWatcher,
            sequencer_manager = SeqManager,
            async_request_manager = AsyncReqManager,
            connections = [Conn]
        }
    }} = ?assertMatch(
        {ok, #document{value = #session{node = WorkerToCheck, connections = [_]}}},
        rpc:call(WorkerToKill, session, get, [SessId]),
        Attempts
    ),

    % Assert that all session processes were recreated on WorkerToCheck (connection
    % process persisted on that node)
    assert_processes_reside_on_node(WorkerToCheck, [
        Sup, EventManager, SessionWatcher, SeqManager, AsyncReqManager, Conn
    ]),

    assert_proper_connections_rib(SessId, AsyncReqManager, [Conn]),

    lists:foreach(fun
        ({Worker, Sock}) when Worker == WorkerToKill ->
            ?assertEqual({error, closed}, ssl:send(Sock, <<"msg">>));
        ({Worker, Sock}) when Worker == WorkerToCheck ->
            fuse_test_utils:ls(Sock, SpaceGuid)
    end, ConnectionPerWorker).

assert_processes_reside_on_node(Node, Processes) ->
    lists:foreach(fun(Process) -> ?assertEqual(Node, node(Process)) end, Processes).

assert_proper_connections_rib(SessId, AsyncReqManager, Cons) ->
    lists:foreach(fun(Conn) ->
        ?assertEqual({rib, {Conn, AsyncReqManager, SessId}}, get_connection_rib(Conn))
    end, Cons).

get_connection_rib(Conn) ->
    element(15, element(4, sys:get_state(Conn))).
