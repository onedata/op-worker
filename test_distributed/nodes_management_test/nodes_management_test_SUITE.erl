%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test creates many Erlang virtual machines and uses them
%% to test how ccm manages workers and monitors nodes.
%% @end
%% ===================================================================

-module(nodes_management_test_SUITE).

-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([one_node_test/1, ccm_and_worker_test/1]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [one_node_test, ccm_and_worker_test]. %todo refill

%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

ccm_code1() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  ok.

ccm_code2() ->
  gen_server:cast({global, ?CCM}, init_cluster),
  ok.

worker_code() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  ok.

%% ====================================================================
%% Test function
%% ====================================================================
one_node_test(Config) ->
    [Node] = ?config(nodes, Config),
    ?assertMatch(ccm, gen_server:call({?Node_Manager_Name, Node}, getNodeType)).

ccm_and_worker_test(Config) ->
    [Ccm, Worker] = ?config(nodes, Config),
    gen_server:call({?Node_Manager_Name, Ccm}, getNodeType),
    ?assertMatch(ccm, gen_server:call({?Node_Manager_Name, Ccm}, getNodeType)),
    ?assertMatch(worker, gen_server:call({?Node_Manager_Name, Worker}, getNodeType)),

    timer:sleep(15000), %todo reorganize cluster startup, so we don't have to wait

    ?assertEqual(ok, gen_server:call({?Dispatcher_Name, Ccm}, {http_worker, 1, self(), ping})),
    ?assertEqual(pong, receive Msg -> Msg end),
    ?assertEqual(ok, gen_server:call({?Dispatcher_Name, Worker}, {http_worker, 1, self(), ping})),
    ?assertEqual(pong, receive Msg -> Msg end).


%% main_test(Config) ->
%%   NodesUp = ?config(nodes, Config).
%%
%%   [CCM | WorkerNodes] = NodesUp,
%%
%%   ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
%%   test_utils:wait_for_cluster_cast(),
%%   RunWorkerCode = fun(Node) ->
%%     ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
%%     test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
%%   end,
%%   lists:foreach(RunWorkerCode, WorkerNodes),
%%   ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
%%
%%   NotExistingNodes = ['n1@localhost', 'n2@localhost', 'n3@localhost'],
%%   lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NotExistingNodes),
%%   test_utils:wait_for_cluster_cast(),
%%   Nodes = gen_server:call({global, ?CCM}, get_nodes, 500),
%%   ?assertEqual(length(Nodes), length(NodesUp)),
%%     lists:foreach(fun(Node) ->
%%       ?assert(lists:member(Node, Nodes))
%%     end, NodesUp),
%%
%%   lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NodesUp),
%%   test_utils:wait_for_cluster_cast(),
%%   Nodes2 = gen_server:call({global, ?CCM}, get_nodes, 500),
%%   ?assertEqual(length(Nodes2), length(NodesUp)),
%%
%%   {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers, 1000),
%%   PeerCert = ?COMMON_FILE("peer.pem"),
%%   Ping = #atom{value = "ping"},
%%   PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),
%%
%%   Pong = #atom{value = "pong"},
%%   PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
%%   PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
%%   PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),
%%
%%   Ports = [5055, 6666, 7777, 8888],
%%   CheckNodes = fun(Port, S) ->
%%     {ConAns, Socket} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
%%     ?assertEqual(ok, ConAns),
%%
%%     CheckModules = fun(M, Sum) ->
%%       Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom",
%%       message_decoder_name = "communication_protocol", answer_type = "atom",
%%       answer_decoder_name = "communication_protocol", protocol_version = 1, input = PingBytes},
%%       Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),
%%
%%       ok = wss:send(Socket, Msg),
%%       {RecvAns, Ans} = wss:recv(Socket, 5000),
%%       ?assertEqual(ok, RecvAns),
%%       case Ans =:= PongAnsBytes of
%%         true -> Sum + 1;
%%         false -> Sum
%%       end
%%     end,
%%
%%     PongsNum = lists:foldl(CheckModules, 0, Jobs),
%%     wss:close(Socket),
%%     S + PongsNum
%%   end,
%%
%%   PongsNum2 = lists:foldl(CheckNodes, 0, Ports),
%%   ?assertEqual(PongsNum2, length(Jobs) * length(Ports)).

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(one_node_test, Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    [Node] = test_node_starter:start_test_nodes(1),
    DBNode = ?DB_NODE,

    test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, [Node], [
        [{node_type, ccm}, {dispatcher_port, 8888}, {ccm_nodes, [Node]}, {db_nodes, [DBNode]}, {heart_beat, 1}]]),

    lists:append([{nodes, [Node]}, {dbnode, DBNode}], Config);

init_per_testcase(ccm_and_worker_test, Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    Nodes = [Ccm, _] = test_node_starter:start_test_nodes(2, true),
    DBNode = ?DB_NODE,

    test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes, [
        [{node_type, ccm}, {dispatcher_port, 8888}, {ccm_nodes, [Ccm]}, {db_nodes, [DBNode]}, {heart_beat, 1}],
        [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [Ccm]}, {db_nodes, [DBNode]}, {heart_beat, 1}]
    ]),

    lists:append([{nodes, Nodes}, {dbnode, DBNode}], Config);

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = ?DB_NODE,

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {control_panel_port, 1350}, {control_panel_redirect_port, 1354}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {rest_port, 8443}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {control_panel_port, 1351}, {control_panel_redirect_port, 1355}, {gateway_listener_port, 3219}, {gateway_proxy_port, 3220}, {rest_port, 8444}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {control_panel_port, 1352}, {control_panel_redirect_port, 1356}, {gateway_listener_port, 3221}, {gateway_proxy_port, 3222}, {rest_port, 8445}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 8888}, {control_panel_port, 1353}, {control_panel_redirect_port, 1357}, {gateway_listener_port, 3223}, {gateway_proxy_port, 3224}, {rest_port, 8446}, {ccm_nodes, [CCM]}, {dns_port, 1311}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}]]),

  lists:append([{nodes, NodesUp}, {dbnode, DBNode}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().