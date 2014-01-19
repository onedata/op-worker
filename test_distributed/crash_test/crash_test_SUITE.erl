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
-include("nodes_manager.hrl").
-include("fuse_messages_pb.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([main_test/1, callbacks_test/1]).

all() -> [main_test, callbacks_test].

%% ====================================================================
%% Test function
%% ====================================================================

main_test(Config) ->
  nodes_manager:check_start_assertions(Config),
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

  timer:sleep(6000),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node)),

  Jobs = ?Modules,
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
      answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
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
    S + TmpPongsNum
  end,

  {Workers, InitialStateNum} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(length(Workers), length(Jobs)),
  PongsNum = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum, length(Jobs) * length(Ports)),

  nodes_manager:stop_node(CCM),
  timer:sleep(7000),
  ?assertEqual(CCM2, gen_server:call({global, ?CCM}, get_ccm_node)),

  {Workers2, StateNum2} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(InitialStateNum + 1, StateNum2),
  ?assertEqual(length(Workers2), length(Jobs)),
  PongsNum2 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum2, length(Jobs) * length(Ports)),

  {StartAns, NewNode} = nodes_manager:start_node(CCM, CCMParams),
  ?assertEqual(ok, StartAns),
  ?assertEqual(CCM, NewNode),
  StartLog = nodes_manager:start_app_on_nodes([CCM], [CCMArgs]),
  ?assertEqual(false, lists:member(error, StartLog)),

  timer:sleep(7000),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node)),

  {Workers3, StateNum3} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(InitialStateNum + 2, StateNum3),
  ?assertEqual(length(Workers3), length(Jobs)),
  PongsNum3 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum3, length(Jobs) * length(Ports)),

  nodes_manager:stop_node(Worker1),
  timer:sleep(2000),

  Ports2 = [8888],
  {Workers4, StateNum4} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(InitialStateNum + 4, StateNum4),
  ?assertEqual(length(Workers4), length(Jobs)),
  PongsNum4 = lists:foldl(CheckNodes, 0, Ports2),
  ?assertEqual(PongsNum4, length(Jobs) * length(Ports2)),

  {StartAns2, NewNode2} = nodes_manager:start_node(Worker1, WorkerParams),
  ?assertEqual(ok, StartAns2),
  ?assertEqual(Worker1, NewNode2),
  StartLog2 = nodes_manager:start_app_on_nodes([Worker1], [WorkerArgs]),
  ?assertEqual(false, lists:member(error, StartLog2)),
  timer:sleep(2000),

  {Workers5, StateNum5} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(InitialStateNum + 4, StateNum5),
  ?assertEqual(length(Workers5), length(Jobs)),
  PongsNum5 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum5, length(Jobs) * length(Ports)).


callbacks_test(Config) ->
  nodes_manager:check_start_assertions(Config),
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

  timer:sleep(6000),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node)),

  PeerCert = ?COMMON_FILE("peer.pem"),

  %% Add test users since cluster wont generate FuseId without full authentication
  {ReadFileAns, PemBin} = file:read_file(PeerCert),
  ?assertEqual(ok, ReadFileAns),
  {ExtractAns, RDNSequence} = rpc:call(Worker1, user_logic, extract_dn_from_cert, [PemBin]),
  ?assertEqual(rdnSequence, ExtractAns),
  {ConvertAns, DN} = rpc:call(Worker1, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
  ?assertEqual(ok, ConvertAns),
  DnList = [DN],

  Login = "user1",
  Name = "user1 user1",
  Teams = ["user1 team"],
  Email = "user1@email.net",
  {CreateUserAns, _} = rpc:call(Worker1, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),
  %% END Add user

  {ok, Socket0} = wss:connect('localhost', 7777, [{certfile, PeerCert}, {cacertfile, PeerCert}]),
  FuseId1 = wss:handshakeInit(Socket0, "hostname", []), %% Get first fuseId
  FuseId2 = wss:handshakeInit(Socket0, "hostname", []), %% Get second fuseId
  wss:close(Socket0),

  Reg1 = #channelregistration{fuse_id = FuseId1},
  Reg1Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelregistration(Reg1)),
  Message1 = #clustermsg{module_name = "fslogic", message_type = "channelregistration",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = Reg1Bytes},
  Msg1 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message1)),

  Reg2 = #channelregistration{fuse_id = FuseId2},
  Reg2Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelregistration(Reg2)),
  Message2 = #clustermsg{module_name = "fslogic", message_type = "channelregistration",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = Reg2Bytes},
  Msg2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),

  Ans = #atom{value = "ok"},
  AnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ans)),
  RegAns = #answer{answer_status = "ok", worker_answer = AnsBytes},
  RegAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(RegAns)),

  {_, InitialCallbacksNum} = gen_server:call({global, ?CCM}, get_callbacks),

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

  lists:foldl(RegisterCallbacks, [], Ports),
  timer:sleep(1000),

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
    Test1 = gen_server:call({?Dispatcher_Name, Node}, get_callbacks),
    CheckDispatcherAns(DispatcherCorrectAns, Test1),
    Test2 = gen_server:call({?Node_Manager_Name, Node}, get_fuses_list),
    ?assertEqual(FusesList, Test2),
    Test3 = gen_server:call({?Node_Manager_Name, Node}, {get_all_callbacks, FuseId1}),
    ?assertEqual(Fuse1AnsLength, length(Test3)),
    Test4 = gen_server:call({?Node_Manager_Name, Node}, {get_all_callbacks, FuseId2}),
    ?assertEqual(Fuse2AnsLength, length(Test4)),

    DispatcherCorrectAns
  end,

  DispatcherCorrectAns1 = {[{FuseId1, lists:reverse(WorkerNodes)}, {FuseId2, [Worker1]}], InitialCallbacksNum + 3},
  FuseInfo1 = [{[FuseId1, FuseId2], 2,1}, {[FuseId1], 1,0}],
  CCMTest1 = gen_server:call({global, ?CCM}, get_callbacks),
  CheckDispatcherAns(DispatcherCorrectAns1, CCMTest1),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns1, lists:zip(WorkerNodes, FuseInfo1)),

  nodes_manager:stop_node(CCM),
  timer:sleep(7000),
  ?assertEqual(CCM2, gen_server:call({global, ?CCM}, get_ccm_node)),

  DispatcherCorrectAns2 = {[{FuseId1, lists:reverse(WorkerNodes)}, {FuseId2, [Worker1]}], InitialCallbacksNum + 4},
  CCMTest2 = gen_server:call({global, ?CCM}, get_callbacks),
  CheckDispatcherAns(DispatcherCorrectAns2, CCMTest2),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns2, lists:zip(WorkerNodes, FuseInfo1)),

  {StartAns, NewNode} = nodes_manager:start_node(CCM, CCMParams),
  ?assertEqual(ok, StartAns),
  ?assertEqual(CCM, NewNode),
  StartLog = nodes_manager:start_app_on_nodes([CCM], [CCMArgs]),
  ?assertEqual(false, lists:member(error, StartLog)),

  timer:sleep(7000),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node)),

  DispatcherCorrectAns3 = {[{FuseId1, lists:reverse(WorkerNodes)}, {FuseId2, [Worker1]}], InitialCallbacksNum + 5},
  CCMTest3 = gen_server:call({global, ?CCM}, get_callbacks),
  CheckDispatcherAns(DispatcherCorrectAns3, CCMTest3),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns3, lists:zip(WorkerNodes, FuseInfo1)),



  nodes_manager:stop_node(Worker1),
  timer:sleep(2000),

  DispatcherCorrectAns4 = {[{FuseId1, Worker2}], InitialCallbacksNum + 6},
  FuseInfo4 = [{[FuseId1], 1,0}],
  CCMTest4 = gen_server:call({global, ?CCM}, get_callbacks),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest4),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),



  {StartAns2, NewNode2} = nodes_manager:start_node(Worker1, WorkerParams),
  ?assertEqual(ok, StartAns2),
  ?assertEqual(Worker1, NewNode2),
  StartLog2 = nodes_manager:start_app_on_nodes([Worker1], [WorkerArgs]),
  ?assertEqual(false, lists:member(error, StartLog2)),
  timer:sleep(2000),

  CCMTest5 = gen_server:call({global, ?CCM}, get_callbacks),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest5),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),

  nodes_manager:stop_node(Worker1),
  timer:sleep(2000),

  CCMTest6 = gen_server:call({global, ?CCM}, get_callbacks),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest6),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),

  {StartAns3, NewNode3} = nodes_manager:start_node(Worker1, WorkerParams),
  ?assertEqual(ok, StartAns3),
  ?assertEqual(Worker1, NewNode3),
  StartLog3 = nodes_manager:start_app_on_nodes([Worker1], [WorkerArgs]),
  ?assertEqual(false, lists:member(error, StartLog3)),
  timer:sleep(2000),

  CCMTest7 = gen_server:call({global, ?CCM}, get_callbacks),
  CheckDispatcherAns(DispatcherCorrectAns4, CCMTest7),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns4, lists:zip(Worker2, FuseInfo4)),

  rpc:call(CCM, user_logic, remove_user, [{dn, DN}]).

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  {NodesUp, Params} = nodes_manager:start_test_on_nodes_with_dist_app(4, 2),
  [CCM | NodesUp2] = NodesUp,
  [CCM2 | _] = NodesUp2,
  DB_Node = nodes_manager:get_db_node(),
  Args = [[{node_type, ccm}, {dispatcher_port, 5055}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1308}, {db_nodes, [DB_Node]}, {initialization_time, 5}, {cluster_clontrol_period, 1}, {heart_beat, 1}],
    [{node_type, ccm}, {dispatcher_port, 6666}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1309}, {db_nodes, [DB_Node]}, {initialization_time, 5}, {cluster_clontrol_period, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1310}, {db_nodes, [DB_Node]}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [CCM, CCM2]}, {dns_port, 1311}, {db_nodes, [DB_Node]}, {heart_beat, 1}]],

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, Args),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {params, Params}, {args, Args}, {assertions, Assertions}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).
