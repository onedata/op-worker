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
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-define(ProtocolVersion, 1).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([fuse_session_cleanup_test/1, main_test/1, callbacks_test/1, fuse_ack_routing_test/1, workers_list_singleton_and_permanent_test/1]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [fuse_session_cleanup_test, main_test, callbacks_test, fuse_ack_routing_test, workers_list_singleton_and_permanent_test].

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

%% This test checks if acks from fuse are routed correctly
fuse_ack_routing_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, fslogic}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, fslogic, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  test_utils:wait_for_cluster_init(length(NodesUp) - 1),

  ?ENABLE_PROVIDER(Config),

  [Worker1, Worker2, Worker3] = WorkerNodes,

  PeerCert = ?COMMON_FILE("peer.pem"),

  %% Add test users since cluster wont generate FuseId without full authentication
  UserDoc = test_utils:add_user(Config, "user1", PeerCert, ["space1"]),

  {ConAns0, Socket0} = wss:connect('localhost', 6666, [{certfile, PeerCert}, {cacertfile, PeerCert}]),
  ?assertEqual(ok, ConAns0),
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

  FuseId1Port = 5055,
  FuseId2Port = 7777,
  TestPort1 = 6666,
  TestPort2 = 8888,

  {ConAns, Socket} = wss:connect('localhost', FuseId1Port, [{certfile, PeerCert}]),
  ?assertEqual(ok, ConAns),

  HandshakeRes = wss:handshakeAck(Socket, FuseId1), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes),

  ?assertEqual(ok, wss:send(Socket, Msg1)),
  {RecvAns, SendAns} = wss:recv(Socket, 5000),
  ?assertEqual(ok, RecvAns),
  ?assertEqual(RegAnsBytes, SendAns),

  {ConAns2, Socket2} = wss:connect('localhost', FuseId2Port, [{certfile, PeerCert}]),
  ?assertEqual(ok, ConAns2),

  HandshakeRes2 = wss:handshakeAck(Socket2, FuseId2), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes2),

  ?assertEqual(ok, wss:send(Socket2, Msg2)),
  {RecvAns2, SendAns2} = wss:recv(Socket2, 5000),
  ?assertEqual(ok, RecvAns2),
  ?assertEqual(RegAnsBytes, SendAns2),

  test_utils:wait_for_request_handling(),

  Pid = self(),
  OnCompleteCallback = fun(SucessFuseIds, FailFuseIds) ->
    Pid ! {on_complete_callback, length(SucessFuseIds), length(FailFuseIds), node()}
  end,

  MsgAtom = #atom{value = "some_message"},
  ?assertEqual(ok, rpc:call(Worker2, worker_host, send_to_user_with_ack, [{uuid, UserDoc#db_document.uuid}, MsgAtom, "communication_protocol", OnCompleteCallback, ?ProtocolVersion])),

  MsgId1 = gen_server:call({global, ?CCM}, {node_for_ack, node()}) + 1,

  MsgAtomBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(MsgAtom)),
  MsgAtomAns = #answer{answer_status = "push", worker_answer = MsgAtomBytes, message_type = "atom", message_id = MsgId1},
  MessageAtomAns = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(MsgAtomAns)),

  Ack = #atom{value = "ack"},
  AckBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ack)),
  AckMessage = #clustermsg{module_name = "fslogic", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = MsgId1, input = AckBytes},
  AckMessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(AckMessage)),

  {CallbackRecvAns, CallbackAns} = wss:recv(Socket, 1000),
  ?assertEqual(ok, CallbackRecvAns),
  ?assertEqual(MessageAtomAns, CallbackAns),

  {CallbackRecvAns2, CallbackAns2} = wss:recv(Socket2, 1000),
  ?assertEqual(ok, CallbackRecvAns2),
  ?assertEqual(MessageAtomAns, CallbackAns2),

  {ConAns3, Socket3} = wss:connect('localhost', TestPort1, [{certfile, PeerCert}]),
  HandshakeRes3 = wss:handshakeAck(Socket3, FuseId1), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes3),
  ?assertEqual(ok, ConAns3),
  ?assertEqual(ok, wss:send(Socket3, AckMessageBytes)),

  {ConAns4, Socket4} = wss:connect('localhost', TestPort2, [{certfile, PeerCert}]),
  HandshakeRes4 = wss:handshakeAck(Socket4, FuseId2), %% Set fuseId for this connection
  ?assertEqual(ok, HandshakeRes4),
  ?assertEqual(ok, ConAns4),
  ?assertEqual(ok, wss:send(Socket4, AckMessageBytes)),

  ?assertEqual([Worker2], check_answers(1)),

  ?assertEqual(ok, rpc:call(Worker2, worker_host, send_to_user_with_ack, [{uuid, UserDoc#db_document.uuid}, MsgAtom, "communication_protocol", OnCompleteCallback, ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(Worker2, worker_host, send_to_user_with_ack, [{uuid, UserDoc#db_document.uuid}, MsgAtom, "communication_protocol", OnCompleteCallback, ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(Worker1, worker_host, send_to_user_with_ack, [{uuid, UserDoc#db_document.uuid}, MsgAtom, "communication_protocol", OnCompleteCallback, ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(Worker3, worker_host, send_to_user_with_ack, [{uuid, UserDoc#db_document.uuid}, MsgAtom, "communication_protocol", OnCompleteCallback, ?ProtocolVersion])),
  MsgId2 = MsgId1 - 2,
  MsgId3 = MsgId2 - 1,
  MsgId4 = MsgId3 - 1,
  MsgId5 = MsgId4 - 1,

  MsgAtomAns2 = #answer{answer_status = "push", worker_answer = MsgAtomBytes, message_type = "atom", message_id = MsgId2},
  MessageAtomAns2 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(MsgAtomAns2)),

  MsgAtomAns3 = #answer{answer_status = "push", worker_answer = MsgAtomBytes, message_type = "atom", message_id = MsgId3},
  MessageAtomAns3 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(MsgAtomAns3)),

  MsgAtomAns4 = #answer{answer_status = "push", worker_answer = MsgAtomBytes, message_type = "atom", message_id = MsgId4},
  MessageAtomAns4 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(MsgAtomAns4)),

  MsgAtomAns5 = #answer{answer_status = "push", worker_answer = MsgAtomBytes, message_type = "atom", message_id = MsgId5},
  MessageAtomAns5 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(MsgAtomAns5)),

  AckMessage2 = #clustermsg{module_name = "fslogic", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = MsgId2, input = AckBytes},
  AckMessageBytes2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(AckMessage2)),

  AckMessage3 = #clustermsg{module_name = "fslogic", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = MsgId3, input = AckBytes},
  AckMessageBytes3 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(AckMessage3)),

  AckMessage4 = #clustermsg{module_name = "fslogic", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = MsgId4, input = AckBytes},
  AckMessageBytes4 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(AckMessage4)),

  AckMessage5 = #clustermsg{module_name = "fslogic", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = MsgId5, input = AckBytes},
  AckMessageBytes5 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(AckMessage5)),

  {CallbackRecvAns3, CallbackAns3} = wss:recv(Socket, 1000),
  ?assertEqual(ok, CallbackRecvAns3),
  {CallbackRecvAns4, CallbackAns4} = wss:recv(Socket, 1000),
  ?assertEqual(ok, CallbackRecvAns4),
  {CallbackRecvAns5, CallbackAns5} = wss:recv(Socket, 1000),
  ?assertEqual(ok, CallbackRecvAns5),
  {CallbackRecvAns6, CallbackAns6} = wss:recv(Socket, 1000),
  ?assertEqual(ok, CallbackRecvAns6),
  CallbackAnsList = [CallbackAns3, CallbackAns4, CallbackAns5, CallbackAns6],
  MessagesList = [MessageAtomAns2, MessageAtomAns3, MessageAtomAns4, MessageAtomAns5],
  lists:foreach(fun(M) -> ?assert(lists:member(M, CallbackAnsList)) end, MessagesList),

  {CallbackRecvAns7, CallbackAns7} = wss:recv(Socket2, 1000),
  ?assertEqual(ok, CallbackRecvAns7),
  {CallbackRecvAns8, CallbackAns8} = wss:recv(Socket2, 1000),
  ?assertEqual(ok, CallbackRecvAns8),
  {CallbackRecvAns9, CallbackAns9} = wss:recv(Socket2, 1000),
  ?assertEqual(ok, CallbackRecvAns9),
  {CallbackRecvAns10, CallbackAns10} = wss:recv(Socket2, 1000),
  ?assertEqual(ok, CallbackRecvAns10),
  CallbackAnsList2 = [CallbackAns7, CallbackAns8, CallbackAns9, CallbackAns10],
  lists:foreach(fun(M) -> ?assert(lists:member(M, CallbackAnsList2)) end, MessagesList),

  ?assertEqual(ok, wss:send(Socket3, AckMessageBytes2)),
  ?assertEqual(ok, wss:send(Socket3, AckMessageBytes3)),
  ?assertEqual(ok, wss:send(Socket3, AckMessageBytes4)),
  ?assertEqual(ok, wss:send(Socket3, AckMessageBytes5)),
  ?assertEqual(ok, wss:send(Socket4, AckMessageBytes4)),
  ?assertEqual(ok, wss:send(Socket4, AckMessageBytes3)),
  ?assertEqual(ok, wss:send(Socket4, AckMessageBytes2)),
  ?assertEqual(ok, wss:send(Socket4, AckMessageBytes5)),

  AnsNodes = check_answers(4),
  ?assertEqual(4, length(AnsNodes)),
  ?assertEqual(false, lists:member(error, AnsNodes)),
  AnsNodes2 = lists:delete(Worker2, AnsNodes),
  ?assertEqual(3, length(AnsNodes2)),
  lists:foreach(fun(M) -> ?assert(lists:member(M, AnsNodes2)) end, WorkerNodes),

  wss:close(Socket),
  wss:close(Socket2),
  wss:close(Socket3).


%% This test checks if FUSE sessions are cleared properly
fuse_session_cleanup_test(Config) ->
    NodesUp = ?config(nodes, Config),
    DBNode = ?config(dbnode, Config),
    [CCM | WorkerNodes] = NodesUp,

    DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),

    ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
    test_utils:wait_for_cluster_cast(),
    RunWorkerCode = fun(Node) ->
      ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
      test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
    end,
    lists:foreach(RunWorkerCode, WorkerNodes),
    ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
    test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

    ?ENABLE_PROVIDER(Config),

    %% Worker ports: 6666, 7777, 8888
    Host = "localhost",
    SpaceName = "user1 team",

    Cert1 = ?COMMON_FILE("peer.pem"),
    _Cert2 = ?COMMON_FILE("peer2.pem"),

    %% Add test users since cluster wont generate FuseId without full authentication
    UserDoc = test_utils:add_user(Config, "user1", Cert1, [SpaceName]),

    %% Open connections for the user as session #1
    {ConAns11, Socket11} = wss:connect(Host, 6666, [{certfile, Cert1}, {cacertfile, Cert1}]), %% Node #1
    ?assertEqual(ok, ConAns11),
    {ConAns12, Socket12} = wss:connect(Host, 7777, [{certfile, Cert1}, {cacertfile, Cert1}]), %% Node #2
    ?assertEqual(ok, ConAns12),
    {ConAns13, Socket13} = wss:connect(Host, 7777, [{certfile, Cert1}, {cacertfile, Cert1}]), %% Node #2
    ?assertEqual(ok, ConAns13),
    FuseID1 = wss:handshakeInit(Socket11, "hostname1", []),

    ?assertEqual(ok, wss:handshakeAck(Socket11, FuseID1)),
    ?assertEqual(ok, wss:handshakeAck(Socket12, FuseID1)),
    ?assertEqual(ok, wss:handshakeAck(Socket13, FuseID1)),

    %% Open connections for the user as session #2
    {ConAns21, Socket21} = wss:connect(Host, 7777, [{certfile, Cert1}, {cacertfile, Cert1}]), %% Node #2
    ?assertEqual(ok, ConAns21),
    {ConAns22, Socket22} = wss:connect(Host, 8888, [{certfile, Cert1}, {cacertfile, Cert1}]), %% Node #3
    ?assertEqual(ok, ConAns22),
    FuseID2 = wss:handshakeInit(Socket21, "hostname2", []),

    ?assertEqual(ok, wss:handshakeAck(Socket21, FuseID2)),
    ?assertEqual(ok, wss:handshakeAck(Socket22, FuseID2)),

    %% Check if everithing is fine in DB
    {Status0, Ans0} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID1}], 1]),
    {Status1, Ans1} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID2}], 1]),
    {Status2, Ans2} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_fuse_sessions, [{by_valid_to, utils:time() + 60}], 1]),
    ?assertEqual([ok, ok, ok], [Status0, Status1, Status2]),

    ?assertEqual(3, length(Ans0)),
    ?assertEqual(2, length(Ans1)),
    ?assertEqual(2, length(Ans2)),

    %% Close some connections
    wss:close(Socket11),
    wss:close(Socket13),

    test_utils:wait_for_fuse_session_exp(),

    %% Check if everithing is fine in DB
    {Status3, Ans3} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID1}], 1]),
    {Status4, Ans4} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID2}], 1]),
    {Status5, Ans5} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_fuse_sessions, [{by_valid_to, utils:time() + 60}], 1]),
    ?assertEqual([ok, ok, ok], [Status3, Status4, Status5]),

    ?assertEqual(1, length(Ans3)),
    ?assertEqual(2, length(Ans4)),
    ?assertEqual(2, length(Ans5)),

    %% Close last connection for session #1
    wss:close(Socket12),

    test_utils:wait_for_fuse_session_exp(),

    %% Check if everithing is fine in DB
    {Status6, Ans6} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID1}], 1]),
    {Status7, Ans7} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID2}], 1]),
    {Status8, Ans8} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_fuse_sessions, [{by_valid_to, utils:time() + 60}], 1]),
    ?assertEqual([ok, ok, ok], [Status6, Status7, Status8]),

    ?assertEqual(0, length(Ans6)),
    ?assertEqual(2, length(Ans7)),
    ?assertEqual(1, length(Ans8)),


    %% Stop dao - info will not be cleared from DB during socket closing (check if cache clearing procedure will clear it)
    DaoStop = rpc:call(CCM, dao_lib, apply, [dao_hosts, delete, [DBNode], ?ProtocolVersion]),
    ?assertEqual(ok, DaoStop),

    %% Close connections from session #2
    wss:close(Socket21),
    wss:close(Socket22),

    test_utils:wait_for_request_handling(),
    DaoStart = rpc:call(CCM, dao_lib, apply, [dao_hosts, insert, [DBNode], ?ProtocolVersion]),
    ?assertEqual(ok, DaoStart),

    test_utils:wait_for_fuse_session_exp(),

    %% Check if everithing is fine in DB
    {Status9, Ans9} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID1}], 1]),
    {Status10, Ans10} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_connection_info, [{by_session_id, FuseID2}], 1]),
    {Status11, Ans11} = rpc:call(CCM, dao_lib, apply, [dao_cluster, list_fuse_sessions, [{by_valid_to, utils:time() + 60}], 1]),
    ?assertEqual([ok, ok, ok], [Status9, Status10, Status11]),

    ?assertEqual(0, length(Ans9)),
    ?assertEqual(0, length(Ans10)),
    ?assertEqual(0, length(Ans11)),

    %% Cleanup
    ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ SpaceName], ?ProtocolVersion])),
    ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion])),

    ?assertEqual(ok, rpc:call(CCM, user_logic, remove_user, [{uuid, UserDoc#db_document.uuid}])).

main_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  NotExistingNodes = ['n1@localhost', 'n2@localhost', 'n3@localhost'],
  lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NotExistingNodes),
  test_utils:wait_for_cluster_cast(),
  Nodes = gen_server:call({global, ?CCM}, get_nodes, 500),
  ?assertEqual(length(Nodes), length(NodesUp)),
    lists:foreach(fun(Node) ->
      ?assert(lists:member(Node, Nodes))
    end, NodesUp),

  lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NodesUp),
  test_utils:wait_for_cluster_cast(),
  Nodes2 = gen_server:call({global, ?CCM}, get_nodes, 500),
  ?assertEqual(length(Nodes2), length(NodesUp)),

  {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers, 1000),
  %% @todo: check why dbsync sometimes does not start correctly
  Jobs = ?MODULES -- [dbsync],
  PermamentModules = ?PERMANENT_MODULES,
  ?assert(length(Workers)>= length(Jobs) + 3 * length(PermamentModules)), % 4 slaves

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
      answer_decoder_name = "communication_protocol", protocol_version = 1, input = PingBytes},
      Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

      ok = wss:send(Socket, Msg),
      {RecvAns, Ans} = wss:recv(Socket, 5000),
      ?assertEqual(ok, RecvAns),
      case Ans =:= PongAnsBytes of
        true -> Sum + 1;
        false -> Sum
      end
    end,

    PongsNum = lists:foldl(CheckModules, 0, Jobs),
    wss:close(Socket),
    S + PongsNum
  end,

  PongsNum2 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum2, length(Jobs) * length(Ports)).

callbacks_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [], 2000)),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [], 2000)),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [], 2000)),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  ?ENABLE_PROVIDER(Config),

  [Worker1 | _] = WorkerNodes,

  PeerCert = ?COMMON_FILE("peer.pem"),

  %% Add test users since cluster wont generate FuseId without full authentication
  UserDoc = test_utils:add_user(Config, "user1", PeerCert, ["space1"]),

  {ConAns0, Socket0} = wss:connect('localhost', 6666, [{certfile, PeerCert}, {cacertfile, PeerCert}]),
  ?assertEqual(ok, ConAns0),
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

  UnReg = #channelclose{fuse_id = FuseId1},
  UnRegBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelclose(UnReg)),
  UnMessage = #clustermsg{module_name = "fslogic", message_type = "channelclose",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = UnRegBytes},
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
  test_utils:wait_for_request_handling(),

  CheckDispatcherAns = fun({DispatcherCorrectAnsList, DispatcherCorrectAnsNum}, {TestAnsList, TestAnsNum}) ->
    ?assertEqual(DispatcherCorrectAnsNum, TestAnsNum),
    ?assertEqual(length(DispatcherCorrectAnsList), length(TestAnsList)),
    lists:foreach(fun(FuseNodes) ->
      ?assert(lists:member(FuseNodes, TestAnsList))
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

  DispatcherCorrectAns1 = {[{FuseId1, lists:reverse(NodesUp)}, {FuseId2, [CCM]}], 6},
  FuseInfo1 = [{[FuseId1, FuseId2], 2,1}, {[FuseId1], 1,0}, {[FuseId1], 1,0}, {[FuseId1], 1,0}],
  CCMTest1 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns1, CCMTest1),
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
  test_utils:wait_for_request_handling(),

  [LastNode | _] = lists:reverse(NodesUp),
  DispatcherCorrectAns2 = {[{FuseId1, [LastNode, CCM]}, {FuseId2, [CCM]}], 8},
  FuseInfo2 = [{[FuseId1, FuseId2], 1, 1}, {[], 0,0}, {[], 0,0}, {[FuseId1], 1,0}],
  CCMTest2 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns2, CCMTest2),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns2, lists:zip(NodesUp, FuseInfo2)),

  CallbackSendTest1 = rpc:call(LastNode, request_dispatcher, send_to_fuse, [FuseId2, #atom{value = "test_atom"}, "communication_protocol"], 2000),
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
  answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
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
  test_utils:wait_for_request_handling(),

  DispatcherCorrectAns3 = {[], 11},
  FuseInfo3 = [{[], 0, 0}, {[], 0, 0}, {[], 0, 0}, {[], 0, 0}],
  CCMTest3 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  CheckDispatcherAns(DispatcherCorrectAns3, CCMTest3),
  lists:foldl(CheckCallbacks, DispatcherCorrectAns3, lists:zip(NodesUp, FuseInfo3)),

  rpc:call(Worker1, user_logic, remove_user, [{global_id, UserDoc#db_document{record = #user.global_id}}], 2000).

%% This test checks if workers list inside dispatcher is refreshed correctly.
workers_list_singleton_and_permanent_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [CCM | WorkerNodes] = NodesUp,
  [Worker1 | _] = WorkerNodes,
  Args = ?config(args, Config),
  [_CCMArgs | WorkersArgs] = Args,
  [Worker1Args | _] = WorkersArgs,

  DuplicatedPermanentNodes = (length(WorkerNodes) - 1) * length(?PERMANENT_MODULES),

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, NodesUp),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  ?ENABLE_PROVIDER(Config),

  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  ?assertEqual(CCM, gen_server:call({global, ?CCM}, get_ccm_node, 500)),

  Ok0 = gen_server:cast({global, ?CCM}, {register_module_listener, gateway, {module, gateway}}),
  ?assertEqual(ok, Ok0),


  Self = self(),
  test_utils:ct_mock(Config, gateway, handle, fun(_ProtocolVersion, {node_lifecycle_notification, Node, Module, Action, Pid}) ->
    Self ! {ok, Node, Module, Action, Pid} end),

  test_node_starter:stop_test_nodes([Worker1]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes) - 1),
  test_utils:wait_for_cluster_cast(),
  test_utils:wait_for_cluster_init((length(WorkerNodes) - 2) * length(?PERMANENT_MODULES)),

  receive
    {ok, Node1, Module1, Action1, _Pid1} ->
      ?assertEqual({Worker1, gateway, stop_worker}, {Node1, Module1, Action1})
    after 1000 ->
      ?assert(false)
  end,


  NewNode2 = test_node_starter:start_test_node(?GET_NODE_NAME(Worker1),?GET_HOST(Worker1),false, []),
  ?assertEqual(Worker1, NewNode2),
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, [Worker1], [Worker1Args]),
  test_utils:wait_for_nodes_registration(length(WorkerNodes)),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  receive
    {ok, Node2, Module2, Action2, _Pid2} ->
      ?assertEqual({Worker1, gateway, start_worker}, {Node2, Module2, Action2})
  after 1000 ->
    ?assert(false)
  end.

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(workers_list_singleton_and_permanent_test, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(3),
  [CCM | _] = NodesUp,
  DBNode = ?DB_NODE,
  Args = [
    [{node_type, ccm}, {dispatcher_port, 5055}, {control_panel_port, 1350}, {control_panel_redirect_port, 1354}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {rest_port, 8443}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {control_panel_port, 1351}, {control_panel_redirect_port, 1355}, {gateway_listener_port, 3219}, {gateway_proxy_port, 3220}, {rest_port, 8444}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {control_panel_port, 1352}, {control_panel_redirect_port, 1356}, {gateway_listener_port, 3221}, {gateway_proxy_port, 3222}, {rest_port, 8445}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}]
  ],
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, Args),

  lists:append([{nodes, NodesUp}, {dbnode, DBNode}, {args, Args}], Config);

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

check_answers(0) ->
  [];
check_answers(Num) ->
  receive
    {on_complete_callback, OkNum, ErrorNum, AnsNode} ->
      ?assertEqual(2, OkNum),
      ?assertEqual(0, ErrorNum),
      [AnsNode | check_answers(Num - 1)]
  after 5000 ->
    [error]
  end.
