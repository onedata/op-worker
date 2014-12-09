%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test creates many Erlang virtual machines and uses them
%% to test how cluster works when one or more machines crashes.
%% @end
%% ===================================================================
-module(crash_test_SUITE).

-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").
-include("test_utils.hrl").
-include("fuse_messages_pb.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([main_test/1, callbacks_test/1]).

all() -> [main_test, callbacks_test].

-define(ProtocolVersion, 1).

%% ====================================================================
%% Test function
%% ====================================================================

main_test(Config) ->
  NodesUp = ?config(nodes, Config),
  Params = ?config(params, Config),
  [CCMParams | Params2] = Params,
  Args = ?config(args, Config),
  [CCMArgs | Args2] = Args,

  [CCM | NodesUp2] = NodesUp,
  [CCM2 | WorkerNodes] = NodesUp2,
  [Worker1 | _] = WorkerNodes,

  [_ | Args3] = Args2,
  [WorkerArgs | _] = Args3,

  [_ | Params3] = Params2,
  [WorkerParams | _] = Params3,
  %% @todo: check why dbsync sometimes does not start
  Jobs = ?MODULES -- [dbsync],
  DuplicatedPermanentNodes = (length(WorkerNodes) - 1) * length(?PERMANENT_MODULES),

  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_state_loading(),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node, 500)),


  PeerCert = ?COMMON_FILE("peer.pem"),
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

  Ports = [7777, 8888],
  CheckNodes = fun(Port, S) ->
    {ConAns, Socket} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
    ?assertEqual(ok, ConAns),

    CheckModules = fun(M, Sum) ->
      Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom",
      message_decoder_name = "communication_protocol", answer_type = "atom",
      answer_decoder_name = "communication_protocol", protocol_version = 1, input = PingBytes},
      Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

      wss:send(Socket, Msg),
      {RecvAns, Ans} = wss:recv(Socket, 5000),
      ?assertEqual(ok, RecvAns),
      case Ans =:= PongAnsBytes of
        true -> Sum + 1;
        false -> Sum
      end
    end,

    TmpPongsNum = lists:foldl(CheckModules, 0, Jobs),
	wss:close(Socket),
    S + TmpPongsNum
  end,

  {Workers, InitialStateNum} = gen_server:call({global, ?CCM}, get_workers, 1000),
  ?assertEqual(length(Workers), length(Jobs) + DuplicatedPermanentNodes),
  PongsNum = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum, length(Jobs) * length(Ports)),

  test_node_starter:stop_test_nodes([CCM]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_state_loading(),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  ?assertEqual(CCM2, gen_server:call({global, ?CCM}, get_ccm_node, 500)),

  {Workers2, StateNum2} = gen_server:call({global, ?CCM}, get_workers, 1000),
  ?assertEqual(InitialStateNum + 1, StateNum2),
  ?assertEqual(length(Workers2), length(Jobs) + DuplicatedPermanentNodes),
  PongsNum2 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum2, length(Jobs) * length(Ports)),

  NewNode = test_node_starter:start_test_node(?GET_NODE_NAME(CCM),?GET_HOST(CCM),false, CCMParams),
  ?assertEqual(CCM, NewNode),
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, [CCM], [CCMArgs]),

  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_state_loading(),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node, 500)),

  {Workers3, StateNum3} = gen_server:call({global, ?CCM}, get_workers, 1000),
  ?assertEqual(InitialStateNum + 2, StateNum3),
  ?assertEqual(length(Workers3), length(Jobs) + DuplicatedPermanentNodes),
  PongsNum3 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum3, length(Jobs) * length(Ports)),

  test_node_starter:stop_test_nodes([Worker1]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes) - 1),
  test_utils:wait_for_cluster_init(),

  Ports2 = [8888],
  {Workers4, StateNum4} = gen_server:call({global, ?CCM}, get_workers, 1000),
  ?assertEqual(InitialStateNum + 4, StateNum4),
  ?assertEqual(length(Workers4), length(Jobs)),
  PongsNum4 = lists:foldl(CheckNodes, 0, Ports2),
  ?assertEqual(PongsNum4, length(Jobs) * length(Ports2)),

  NewNode2 = test_node_starter:start_test_node(?GET_NODE_NAME(Worker1),?GET_HOST(Worker1),false,WorkerParams),
  ?assertEqual(Worker1, NewNode2),
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, [Worker1], [WorkerArgs]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {Workers5, StateNum5} = gen_server:call({global, ?CCM}, get_workers, 1000),
  ?assertEqual(InitialStateNum + 4 + 1, StateNum5), %% create new worker
  ?assertEqual(length(Workers5), length(Jobs) + DuplicatedPermanentNodes),
  PongsNum5 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum5, length(Jobs) * length(Ports)).


callbacks_test(Config) ->
  NodesUp = ?config(nodes, Config),
  Params = ?config(params, Config),
  [CCMParams | Params2] = Params,
  Args = ?config(args, Config),
  [CCMArgs | Args2] = Args,

  [CCM | NodesUp2] = NodesUp,
  [CCM2 | WorkerNodes] = NodesUp2,
  [Worker1 | Worker2] = WorkerNodes,

  [_ | Args3] = Args2,
  [WorkerArgs | _] = Args3,

  [_ | Params3] = Params2,
  [WorkerParams | _] = Params3,

  DuplicatedPermanentNodes = (length(WorkerNodes) - 1) * length(?PERMANENT_MODULES),

  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node, 500)),

  ?ENABLE_PROVIDER(Config),

  PeerCert = ?COMMON_FILE("peer.pem"),

  %% Add test users since cluster wont generate FuseId without full authentication
  UserDoc = test_utils:add_user(Config, ?TEST_USER, PeerCert, [?TEST_GROUP]),
  [DN | _] = UserDoc#db_document.record#user.dn_list,

  {ok, Socket0} = wss:connect('localhost', 7777, [{certfile, PeerCert}, {cacertfile, PeerCert}]),
  FuseId1 = wss:handshakeInit(Socket0, "hostname", []), %% Get first fuseId
  FuseId2 = wss:handshakeInit(Socket0, "hostname", []), %% Get second fuseId
  wss:close(Socket0),

  Reg1 = #channelregistration{fuse_id = FuseId1},
  Reg1Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelregistration(Reg1)),
  Message1 = #clustermsg{module_name = "fslogic", message_type = "channelregistration",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = Reg1Bytes},
  Msg1 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message1)),

  Reg2 = #channelregistration{fuse_id = FuseId2},
  Reg2Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelregistration(Reg2)),
  Message2 = #clustermsg{module_name = "fslogic", message_type = "channelregistration",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = Reg2Bytes},
  Msg2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),

  Ans = #atom{value = "ok"},
  AnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ans)),
  RegAns = #answer{answer_status = "ok", worker_answer = AnsBytes},
  RegAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(RegAns)),

  {_, InitialCallbacksNum} = gen_server:call({global, ?CCM}, get_callbacks, 1000),

  Ports = [7777, 8888],
  RegisterCallbacks = fun(Port, Sockets) ->
    {ConAns, Socket} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
    ?assertEqual(ok, ConAns),

    HandshakeRes = wss:handshakeAck(Socket, FuseId1), %% Set fuseId for this connection
    ?assertEqual(ok, HandshakeRes),

    ok = wss:send(Socket, Msg1),
    {RecvAns, SendAns} = wss:recv(Socket, 5000),
    ?assertEqual(ok, RecvAns),
    ?assertEqual(RegAnsBytes, SendAns),

    case Port of
      7777 ->
        {ConAns2, Socket2} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
        ?assertEqual(ok, ConAns2),

        HandshakeRes = wss:handshakeAck(Socket2, FuseId2), %% Set fuseId for this connection
        ?assertEqual(ok, HandshakeRes),

        ok = wss:send(Socket2, Msg2),
        {RecvAns2, SendAns2} = wss:recv(Socket2, 5000),
        ?assertEqual(ok, RecvAns2),
        ?assertEqual(RegAnsBytes, SendAns2),

        {ConAns3, Socket3} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
        ?assertEqual(ok, ConAns3),

        HandshakeRes = wss:handshakeAck(Socket3, FuseId1), %% Set fuseId for this connection
        ?assertEqual(ok, HandshakeRes),

        ok = wss:send(Socket3, Msg1),
        {RecvAns3, SendAns3} = wss:recv(Socket3, 5000),
        ?assertEqual(ok, RecvAns3),
        ?assertEqual(RegAnsBytes, SendAns3),
        [Socket3, Socket, Socket2] ++ Sockets;
      _ ->
        [Socket | Sockets]
    end
  end,

  CreatedSockets = lists:foldl(RegisterCallbacks, [], Ports),
  test_utils:wait_for_request_handling(),

  CheckDispatcherAns = fun({DispatcherCorrectAnsList, DispatcherCorrectAnsNum}, {TestAnsList, TestAnsNum}) ->
    ?assertEqual(DispatcherCorrectAnsNum, TestAnsNum),
    ?assertEqual(length(DispatcherCorrectAnsList), length(TestAnsList)),
    lists:foreach(fun({F_id, FuseNodes}) ->
      ?assert(lists:member(F_id, proplists:get_keys(TestAnsList))),
      lists:foreach(fun(FuseNode) ->
        ?assert(lists:member(FuseNode, FuseNodes))
      end, proplists:get_value(F_id, TestAnsList))
    end, DispatcherCorrectAnsList)
  end,

  CheckCallbacks = fun({Node, {FusesList, Fuse1AnsLength, Fuse2AnsLength}}, DispatcherCorrectAns) ->
    Test1 = gen_server:call({?Dispatcher_Name, Node}, get_callbacks, 1000),
    CheckDispatcherAns(DispatcherCorrectAns, Test1),
    Test2 = gen_server:call({?Node_Manager_Name, Node}, get_fuses_list, 1000),
    ?assertEqual(FusesList, Test2),
    Test3 = gen_server:call({?Node_Manager_Name, Node}, {get_all_callbacks, FuseId1}, 1000),
    ?assertEqual(Fuse1AnsLength, length(Test3)),
    Test4 = gen_server:call({?Node_Manager_Name, Node}, {get_all_callbacks, FuseId2}, 1000),
    ?assertEqual(Fuse2AnsLength, length(Test4)),

    DispatcherCorrectAns
  end,

  DispatcherCorrectAns1 = {[{FuseId1, lists:reverse(WorkerNodes)}, {FuseId2, [Worker1]}], InitialCallbacksNum + 3},
  FuseInfo1 = [{[FuseId1, FuseId2], 2,1}, {[FuseId1], 1,0}],
  CCMTest1 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns1, CCMTest1),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns1, lists:zip(WorkerNodes, FuseInfo1)),

  lists:foreach(fun(X) -> wss:close(X) end, CreatedSockets),
  test_node_starter:stop_test_nodes([CCM]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_state_loading(),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  ?assertEqual(CCM2, gen_server:call({global, ?CCM}, get_ccm_node, 500)),

  DispatcherCorrectAns2 = {[{FuseId1, lists:reverse(WorkerNodes)}, {FuseId2, [Worker1]}], InitialCallbacksNum + 4},
  CCMTest2 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns2, CCMTest2),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns2, lists:zip(WorkerNodes, FuseInfo1)),

  NewNode = test_node_starter:start_test_node(?GET_NODE_NAME(CCM),?GET_HOST(CCM),false, CCMParams),
  ?assertEqual(CCM, NewNode),
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, [CCM], [CCMArgs]),

  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_state_loading(),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node, 500)),

  DispatcherCorrectAns3 = {[{FuseId1, lists:reverse(WorkerNodes)}, {FuseId2, [Worker1]}], InitialCallbacksNum + 5},
  CCMTest3 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns3, CCMTest3),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns3, lists:zip(WorkerNodes, FuseInfo1)),



  test_node_starter:stop_test_nodes([Worker1]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes) - 1),
  test_utils:wait_for_cluster_init(),

  DispatcherCorrectAns4 = {[{FuseId1, Worker2}], InitialCallbacksNum + 6},
  FuseInfo4 = [{[FuseId1], 1,0}],
  CCMTest4 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest4),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),



  NewNode2 = test_node_starter:start_test_node(?GET_NODE_NAME(Worker1),?GET_HOST(Worker1),false, WorkerParams),
  ?assertEqual(Worker1, NewNode2),
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, [Worker1], [WorkerArgs]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CCMTest5 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest5),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),

  test_node_starter:stop_test_nodes([Worker1]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes) - 1),
  test_utils:wait_for_cluster_init(),

  CCMTest6 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest6),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),

  NewNode3 = test_node_starter:start_test_node(?GET_NODE_NAME(Worker1),?GET_HOST(Worker1),false, WorkerParams),
  ?assertEqual(Worker1, NewNode3),
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, [Worker1], [WorkerArgs]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CCMTest7 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest7),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),

  %% Cleanup
  ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_GROUP], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion])),

  ?assertEqual(ok, rpc:call(CCM, user_logic, remove_user, [{dn, DN}])).

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  {NodesUp, Params} = test_node_starter:start_test_nodes_with_dist_app(4, 2),
  [CCM | NodesUp2] = NodesUp,
  [CCM2 | _] = NodesUp2,
  DB_Node = ?DB_NODE,
  Args = [[{node_type, ccm}, {dispatcher_port, 5055}, {control_panel_port, 1350}, {control_panel_redirect_port, 1354}, {rest_port, 8443}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1308}, {db_nodes, [DB_Node]}, {initialization_time, 5}, {cluster_clontrol_period, 1}, {heart_beat, 1}],
    [{node_type, ccm}, {dispatcher_port, 6666}, {control_panel_port, 1351}, {control_panel_redirect_port, 1355}, {rest_port, 8445}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1309}, {db_nodes, [DB_Node]}, {initialization_time, 5}, {cluster_clontrol_period, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {control_panel_port, 1352}, {control_panel_redirect_port, 1356}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {rest_port, 8446}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1310}, {db_nodes, [DB_Node]}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 8888}, {control_panel_port, 1353}, {control_panel_redirect_port, 1357}, {gateway_listener_port, 3219}, {gateway_proxy_port, 3220}, {rest_port, 8447}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1311}, {db_nodes, [DB_Node]}, {heart_beat, 1}]],
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, Args),

  lists:append([{nodes, NodesUp}, {params, Params}, {args, Args}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().
