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

-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([main_test/1, callbacks_test/1]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [main_test %%, callbacks_test %% Test disabled until fixed: VFS-280
].

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

main_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  timer:sleep(500),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, []))
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  timer:sleep(500),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  timer:sleep(500),

  NotExistingNodes = ['n1@localhost', 'n2@localhost', 'n3@localhost'],
  lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NotExistingNodes),
  timer:sleep(100),
  Nodes = gen_server:call({global, ?CCM}, get_nodes),
  ?assertEqual(length(Nodes), length(NodesUp)),
    lists:foreach(fun(Node) ->
      ?assert(lists:member(Node, Nodes))
    end, NodesUp),

  lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NodesUp),
  timer:sleep(100),
  Nodes2 = gen_server:call({global, ?CCM}, get_nodes),
  ?assertEqual(length(Nodes2), length(NodesUp)),

  {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers),
  Jobs = ?Modules,
  ?assertEqual(length(Workers), length(Jobs)),

  PeerCert = ?COMMON_FILE("peer.pem"),
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

  Ports = [5055, 6666, 7777, 8888],
  CheckNodes = fun(Port, S) ->
    {ConAns, Socket} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
    ?assertEqual(ok, ConAns),

    CheckModules = fun(M, Sum) ->
      Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom",
      message_decoder_name = "communication_protocol", answer_type = "atom",
      answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
      Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

      ok = wss:send(Socket, Msg),
      {RecvAns, Ans} = wss:recv(Socket, 5000),
      ?assertEqual(ok, RecvAns),
      case Ans =:= PongAnsBytes of
        true -> Sum + 1;
        false -> Sum
      end
    end,

    %% TODO sprawdzić czemu test się wywala jak zamykamy tutaj socket
%%     wss:close(Socket),

    PongsNum = lists:foldl(CheckModules, 0, Jobs),
    S + PongsNum
  end,

  PongsNum2 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum2, length(Jobs) * length(Ports)).

callbacks_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  timer:sleep(500),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, []))
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  timer:sleep(500),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  timer:sleep(1500),

  [Worker1 | _] = WorkerNodes,

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
  Teams = "user1 team",
  Email = "user1@email.net",
  {CreateUserAns, _} = rpc:call(Worker1, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
  ?assertEqual(ok, CreateUserAns),
  %% END Add user

  {ok, Socket0} = wss:connect('localhost', 6666, [{certfile, PeerCert}, {cacertfile, PeerCert}]),
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

  UnReg = #channelclose{fuse_id = FuseId1},
  UnRegBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelclose(UnReg)),
  UnMessage = #clustermsg{module_name = "fslogic", message_type = "channelclose",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = UnRegBytes},
  UnMsg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(UnMessage)),

  SampleAtom = #atom{value = "test_atom"},
  SampleAtomBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(SampleAtom)),
  SampleAtomAns = #answer{answer_status = "push", message_id = -1, message_type ="atom", worker_answer = SampleAtomBytes},
  SampleAtomAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(SampleAtomAns)),

  Ans = #atom{value = "ok"},
  AnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ans)),
  RegAns = #answer{answer_status = "ok", worker_answer = AnsBytes},
  RegAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(RegAns)),

  Ports = [5055, 6666, 7777, 8888],
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
      5055 ->
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

  Callbacks = lists:foldl(RegisterCallbacks, [], Ports),
  timer:sleep(1000),

  CheckCallbacks = fun({Node, {FusesList, Fuse1AnsLength, Fuse2AnsLength}}, DispatcherCorrectAns) ->
    Test1 = gen_server:call({?Dispatcher_Name, Node}, get_callbacks),
    ?assertEqual(DispatcherCorrectAns, Test1),
    Test2 = gen_server:call({?Node_Manager_Name, Node}, get_fuses_list),
    ?assertEqual(FusesList, Test2),
    Test3 = gen_server:call({?Node_Manager_Name, Node}, {get_all_callbacks, FuseId1}),
    ?assertEqual(Fuse1AnsLength, length(Test3)),
    Test4 = gen_server:call({?Node_Manager_Name, Node}, {get_all_callbacks, FuseId2}),
    ?assertEqual(Fuse2AnsLength, length(Test4)),

    DispatcherCorrectAns
  end,

  DispatcherCorrectAns1 = {[{FuseId1, lists:reverse(NodesUp)}, {FuseId2, [CCM]}], 6},
  FuseInfo1 = [{[FuseId1, FuseId2], 2,1}, {[FuseId1], 1,0}, {[FuseId1], 1,0}, {[FuseId1], 1,0}],
  CCMTest1 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual(DispatcherCorrectAns1, CCMTest1),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns1, lists:zip(NodesUp, FuseInfo1)),

  [Node4Connection | Callbacks2] = Callbacks,
  [Fuse2Callback | UnRegCallbacks] = lists:reverse(Callbacks2),
  [_ | UnRegCallbacks2] = UnRegCallbacks,
  UnregisterCallbacks = fun(Socket) ->
    ok = wss:send(Socket, UnMsg),
    {RecvAns, SendAns} = wss:recv(Socket, 5000),
    ?assertEqual(ok, RecvAns),
    ?assertEqual(RegAnsBytes, SendAns)
  end,
  lists:foreach(UnregisterCallbacks, UnRegCallbacks2),
  timer:sleep(1000),

  [LastNode | _] = lists:reverse(NodesUp),
  DispatcherCorrectAns2 = {[{FuseId1, [LastNode, CCM]}, {FuseId2, [CCM]}], 8},
  FuseInfo2 = [{[FuseId1, FuseId2], 1, 1}, {[], 0,0}, {[], 0,0}, {[FuseId1], 1,0}],
  CCMTest2 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual(DispatcherCorrectAns2, CCMTest2),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns2, lists:zip(NodesUp, FuseInfo2)),

  CallbackSendTest1 = rpc:call(LastNode, request_dispatcher, send_to_fuse, [FuseId2, #atom{value = "test_atom"}, "communication_protocol"]),
  ?assertEqual(ok, CallbackSendTest1),
  {CallbackSendTestRecvAns, CallbackSendTestSendAns} = wss:recv(Fuse2Callback, 5000),
  ?assertEqual(ok, CallbackSendTestRecvAns),
  ?assertEqual(SampleAtomAnsBytes, CallbackSendTestSendAns),

  FslogicMessage = #testchannel{answer_delay_in_ms = 3000, answer_message = "CallbackTest"},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_testchannel(FslogicMessage)),

  FuseMessage = #fusemessage{message_type = "testchannel", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = FuseMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  CallbackAns = #testchannelanswer{message = "CallbackTest"},
  CallbackAnsBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_testchannelanswer(CallbackAns)),
  CallbackAnsAns = #answer{answer_status = "push", message_id = -1, message_type ="testchannelanswer", worker_answer = CallbackAnsBytes},
  CallbackAnsAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(CallbackAnsAns)),

  HandshakeRes = wss:handshakeAck(Node4Connection, FuseId2), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  ok = wss:send(Node4Connection, MessageBytes),
  {CallbackSendTestRecvAns2, CallbackSendTestSendAns2} = wss:recv(Node4Connection, 1000),
  ?assertEqual(ok, CallbackSendTestRecvAns2),
  ?assertEqual(RegAnsBytes, CallbackSendTestSendAns2),
  {CallbackSendTestRecvAns3, CallbackSendTestSendAns3} = wss:recv(Fuse2Callback, 1500),
  ?assertEqual(error, CallbackSendTestRecvAns3),
  ?assertEqual(timeout, CallbackSendTestSendAns3),
  {CallbackSendTestRecvAns4, CallbackSendTestSendAns4} = wss:recv(Fuse2Callback, 5000),
  ?assertEqual(ok, CallbackSendTestRecvAns4),
  ?assertEqual(CallbackAnsAnsBytes, CallbackSendTestSendAns4),

  CloseCallbacks = fun(Callback) ->
    wss:close(Callback)
  end,
  lists:foreach(CloseCallbacks, Callbacks),
  timer:sleep(1000),

  DispatcherCorrectAns3 = {[], 11},
  FuseInfo3 = [{[], 0, 0}, {[], 0, 0}, {[], 0, 0}, {[], 0, 0}],
  CCMTest3 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual(DispatcherCorrectAns3, CCMTest3),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns3, lists:zip(NodesUp, FuseInfo3)),

  rpc:call(Worker1, user_logic, remove_user, [{dn, DN}]).

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = nodes_manager:get_db_node(),

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {db_nodes, [DBNode]}],
    [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [CCM]}, {dns_port, 1311}, {db_nodes, [DBNode]}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).