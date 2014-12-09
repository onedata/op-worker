%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test checks if ccm manages workers and if dispatcher
%% has knowledge about workers.
%% @end
%% ===================================================================

-module(ccm_and_dispatcher_test_SUITE).
-include("test_utils.hrl").
-include("registered_names.hrl").
-include("records.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-define(ProtocolVersion, 1).

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([modules_start_and_ping_test/1, dispatcher_connection_test/1, workers_list_actualization_test/1, ping_test/1, application_start_test1/1,
  onedata_handshake_test/1, application_start_test2/1, validation_test/1, callbacks_list_actualization_test/1, monitoring_test/1]).

%% export nodes' codes
-export([application_start_test_code1/0, application_start_test_code2/0]).

all() ->
  [application_start_test1, application_start_test2, modules_start_and_ping_test, workers_list_actualization_test,
    validation_test, ping_test, dispatcher_connection_test, callbacks_list_actualization_test, onedata_handshake_test, monitoring_test].

%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

application_start_test_code1() ->
  application:get_env(?APP_Name, node_type).

application_start_test_code2() ->
  whereis(?Supervisor_Name) /= undefined.

%% ====================================================================
%% Test functions
%% ====================================================================

%% This function tests if ccm application starts properly
application_start_test1(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node | _] = NodesUp,

  ?assertEqual({ok, ccm}, rpc:call(Node, ?MODULE, application_start_test_code1, [])),
  ?assert(rpc:call(Node, ?MODULE, application_start_test_code2, [])).

%% This function tests if worker application starts properly
application_start_test2(Config) ->
  NodesUp = ?config(nodes, Config),
  [Node | _] = NodesUp,

  ?assertEqual({ok, worker}, rpc:call(Node, ?MODULE, application_start_test_code1, [])),
  ?assert(rpc:call(Node, ?MODULE, application_start_test_code2, [])).

%% This function tests if ccm is able to start and connect (using gen_server messages) workers
modules_start_and_ping_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  test_utils:wait_for_cluster_cast({?Node_Manager_Name, CCM}),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  ?assertEqual(1, gen_server:call({global, ?CCM}, get_state_num, 1000)),

  gen_server:cast({global, ?CCM}, get_state_from_db),
  test_utils:wait_for_cluster_cast(),
  ?assertEqual(2, gen_server:call({global, ?CCM}, get_state_num, 1000)),
  State = gen_server:call({global, ?CCM}, get_state, 500),
  Workers = State#cm_state.workers,
  ?assertEqual(1, length(Workers)),

  %% registration of dao dispatcher map
%%   timer:sleep(500),
%%   ?assertEqual(3, gen_server:call({global, ?CCM}, get_state_num)),

  %% registration of dao dispatcher map
  test_utils:wait_for_db_reaction(),

  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),
  State2 = gen_server:call({global, ?CCM}, get_state, 500),
  Workers2 = State2#cm_state.workers,

  %% @todo: check why dbsync sometimes does not start
  Jobs = ?MODULES -- [dbsync],
  ?assert(length(Workers2) >= length(Jobs)),
  ?assertEqual(5, gen_server:call({global, ?CCM}, get_state_num, 1000)),

  CheckModules = fun(M, Sum) ->
    Ans = gen_server:call({M, CCM}, {test_call, ?ProtocolVersion, ping}, 1000),
    case Ans of
      pong -> Sum + 1;
      _Other -> Sum
    end
  end,
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  ?assertEqual(PongsNum, length(Jobs)).

%% This tests check if client may connect to dispatcher.
dispatcher_connection_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,

  PeerCert = ?config(peer_cert, Config),
  Port = ?config(port, Config),

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {ConAns, Socket} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
  ?assertEqual(ok, ConAns),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom", message_decoder_name = "communication_protocol",
    answer_type = "atom", answer_decoder_name = "communication_protocol", protocol_version = 1, input = PingBytes},
  Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, Msg),
  {RecvAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, RecvAns),

  AnsMessage = #answer{answer_status = "wrong_worker_type"},
  AnsMessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(AnsMessage)),

  ?assertEqual(Ans, AnsMessageBytes),

  Message2 = #clustermsg{module_name = "module", message_type = "atom", message_decoder_name = "communication_protocol",
    answer_type = "atom", answer_decoder_name = "communication_protocol", protocol_version = 1, input = PingBytes},
  Msg2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),
  wss:send(Socket, Msg2),
  {RecvAns2, Ans2} = wss:recv(Socket, 5000),
  wss:close(Socket),
  ?assertEqual(ok, RecvAns2),
  ?assertEqual(Ans2, AnsMessageBytes).


%% This test checks if FuseId negotiation works correctly
onedata_handshake_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  ?ENABLE_PROVIDER(Config),

  NodesUp = ?config(nodes, Config),

  Port = ?config(port, Config),
  Host = "localhost",
  SpaceName = "space1",

  Cert1 = ?COMMON_FILE("peer.pem"),
  Cert2 = ?COMMON_FILE("peer2.pem"),

  %% Add test users since cluster wont generate FuseId without full authentication
  UserDoc1 = test_utils:add_user(Config, "user1", Cert1, [SpaceName]),

  %% Open two connections for first user
  {ok, Socket11} = wss:connect(Host, Port, [{certfile, Cert1}, {cacertfile, Cert1}]),
  {ok, Socket12} = wss:connect(Host, Port, [{certfile, Cert1}, {cacertfile, Cert1}]),

  %% Open two connections for second user
  {ok, Socket21} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}]),
  {ok, Socket22} = wss:connect(Host, Port, [{certfile, Cert2}, {cacertfile, Cert2}]),

  %% Negotiate FuseID for user1
  FuseId11 = wss:handshakeInit(Socket11, "hostname1", [{testname1, "testvalue1"}, {testname2, "testvalue2"}]),
  ?assert(is_list(FuseId11)),

  %% Negotiate FuseId for user2 (which not exists)
  ?assertException(throw, no_user_found_error, wss:handshakeInit(Socket21, "hostname2", [])),


  %% Add user2 and renegotiate FuseId
  UserDoc2 = test_utils:add_user(Config, "user2", Cert2, [SpaceName]),
  FuseId21 = wss:handshakeInit(Socket21, "hostname2", []),
  ?assert(is_list(FuseId21)),

  %% Try to use FuseId that not exists
  AnsStatus0 = wss:handshakeAck(Socket11, "someFuseId"),
  ?assertEqual(invalid_fuse_id, AnsStatus0),

  %% Try to use someone else's FuseId
  AnsStatus1 = wss:handshakeAck(Socket11, FuseId21),
  ?assertEqual(invalid_fuse_id, AnsStatus1),

  %% Setup valid fuseId on (almost) all connections
  AnsStatus2 = wss:handshakeAck(Socket11, FuseId11),
  ?assertEqual(ok, AnsStatus2),

  AnsStatus3 = wss:handshakeAck(Socket12, FuseId11),
  ?assertEqual(ok, AnsStatus3),

  AnsStatus4 = wss:handshakeAck(Socket21, FuseId21),
  ?assertEqual(ok, AnsStatus4),


  %% On Socket22 we didnt send ACK, so test if cluster returns error while sending messages that requires FuseId

  %% Channel registration
  Reg2 = #channelregistration{fuse_id = "unused"},
  Reg2Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelregistration(Reg2)),
  Message2 = #clustermsg{module_name = "fslogic", message_type = "channelregistration",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = Reg2Bytes},
  RegMsg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),

  %% Channel close
  UnReg = #channelclose{fuse_id = "unused"},
  UnRegBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_channelclose(UnReg)),
  UnMessage = #clustermsg{module_name = "fslogic", message_type = "channelclose",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = UnRegBytes},
  UnMsg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(UnMessage)),

  %% Fuse message
  FslogicMessage = #renamefile{from_file_logic_name = "file", to_file_logic_name = "file1"},
  FslogicMessageMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_renamefile(FslogicMessage)),
  FuseMessage = #fusemessage{message_type = "renamefile", input = FslogicMessageMessageBytes},
  FuseMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FuseMessage)),
  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
    message_decoder_name = "fuse_messages", answer_type = "atom",
    answer_decoder_name = "communication_protocol", protocol_version = 1, input = FuseMessageBytes},
  FMsgBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),


  %% Send messages above and receive error
  wss:send(Socket22, RegMsg),
  {ok, Data0} = wss:recv(Socket22, 5000),
  #answer{answer_status = Status0} = communication_protocol_pb:decode_answer(Data0),
  ?assertEqual(invalid_fuse_id, list_to_atom(Status0)),

  wss:send(Socket22, UnMsg),
  {ok, Data1} = wss:recv(Socket22, 5000),
  #answer{answer_status = Status1} = communication_protocol_pb:decode_answer(Data1),
  ?assertEqual(invalid_fuse_id, list_to_atom(Status1)),

  wss:send(Socket22, FMsgBytes),
  {ok, Data2} = wss:recv(Socket22, 5000),
  #answer{answer_status = Status2} = communication_protocol_pb:decode_answer(Data2),
  ?assertEqual(invalid_fuse_id, list_to_atom(Status2)),


  %% Now send ACK with FuseId and resend those messages
  AnsStatus5 = wss:handshakeAck(Socket22, FuseId21),
  ?assertEqual(ok, AnsStatus5),

  wss:send(Socket22, RegMsg),
  {ok, Data3} = wss:recv(Socket22, 5000),
  #answer{answer_status = Status3} = communication_protocol_pb:decode_answer(Data3),
  ?assertEqual(ok, list_to_atom(Status3)),

  wss:send(Socket22, UnMsg),
  {ok, Data4} = wss:recv(Socket22, 5000),
  #answer{answer_status = Status4} = communication_protocol_pb:decode_answer(Data4),
  ?assertEqual(ok, list_to_atom(Status4)),

  wss:send(Socket22, FMsgBytes),
  {ok, Data5} = wss:recv(Socket22, 5000),
  #answer{answer_status = Status5} = communication_protocol_pb:decode_answer(Data5),
  ?assertEqual(ok, list_to_atom(Status5)),


  %% Check if session data is correctly stored in DB
  {DAOStatus, DAOAns} = rpc:call(CCM, dao_lib, apply, [dao_cluster, get_fuse_session, [FuseId11], 1]),
  ?assertEqual(ok, DAOStatus),

  #db_document{record = #fuse_session{hostname = Hostname, env_vars = Vars}} = DAOAns,
  ?assertEqual("hostname1", Hostname),
  ?assertEqual([{testname1, "testvalue1"}, {testname2, "testvalue2"}], Vars),

  %% Cleanup
  wss:close(Socket11),
  wss:close(Socket12),
  wss:close(Socket21),
  wss:close(Socket22),
  ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ SpaceName], ?ProtocolVersion])),
  ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/"], ?ProtocolVersion])),

  ?assertEqual(ok, rpc:call(CCM, user_logic, remove_user, [{uuid, UserDoc1#db_document.uuid}])),
  ?assertEqual(ok, rpc:call(CCM, user_logic, remove_user, [{uuid, UserDoc2#db_document.uuid}])).


%% This test checks if workers list inside dispatcher is refreshed correctly.
workers_list_actualization_test(Config) ->
  NodesUp = ?config(nodes, Config),

  %% @todo: check why dbsync sometimes does not start
  Jobs = ?MODULES -- [dbsync],
  [CCM | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  CheckModules = fun(M, Sum) ->
    Workers = gen_server:call({?Dispatcher_Name, CCM}, {get_workers, M}, 1000),
    case (length(Workers) == 1) of %% and lists:member(node(), Workers) of
      true -> Sum + 1;
      false -> Sum
    end
  end,
  OKSum = lists:foldl(CheckModules, 0, Jobs),
  ?assert(OKSum >= length(Jobs)).

%% This test checks if callbacks list inside dispatcher is refreshed correctly.
callbacks_list_actualization_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  Ans1 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 1, #worker_request{subject = "DN", request = #callback{fuse = fuse1, pid = self(), node = CCM, action = channelregistration}}}}, 1000),
  ?assertEqual(ok, Ans1),
  Ans2 = receive
           {worker_answer, 1, WorkerAns} -> WorkerAns
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans2),

  Test1 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}], 2}, Test1),
  Test2 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}], 2}, Test2),
  Test3 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list, 1000),
  ?assertEqual([fuse1], Test3),
  Test4 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse1}, 1000),
  ?assertEqual([self()], Test4),

  Ans3 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 2, #worker_request{subject = "DN", request = #callback{fuse = fuse2, pid = self(), node = CCM, action = channelregistration}}}}, 1000),
  ?assertEqual(ok, Ans3),
  Ans4 = receive
           {worker_answer, 2, WorkerAns2} -> WorkerAns2
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans4),

  Test5 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test5),
  Test6 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test6),
  Test7 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list, 1000),
  ?assertEqual([fuse2, fuse1], Test7),
  Test8 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse2}, 1000),
  ?assertEqual([self()], Test8),

  Ans5 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 3, #worker_request{subject = "DN", request = #callback{fuse = fuse1, pid = pid2, node = CCM, action = channelregistration}}}}, 1000),
  ?assertEqual(ok, Ans5),
  Ans6 = receive
           {worker_answer, 3, WorkerAns3} -> WorkerAns3
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans6),

  Test9 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test9),
  Test10 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test10),
  Test11 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list, 1000),
  ?assertEqual([fuse1, fuse2], Test11),
  Test12 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse1}, 1000),
  ?assertEqual([pid2, self()], Test12),

  Ans7 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 4, #worker_request{subject = "DN", request = #callback{fuse = fuse2, pid = self(), node = CCM, action = channelclose}}}}, 1000),
  ?assertEqual(ok, Ans7),
  Ans8 = receive
           {worker_answer, 4, WorkerAns4} -> WorkerAns4
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans8),

  Test13 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test13),
  Test14 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test14),
  Test15 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list, 1000),
  ?assertEqual([fuse1], Test15),

  Ans9 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 5, #worker_request{subject = "DN", request = #callback{fuse = fuse1, pid = pid2, node = CCM, action = channelclose}}}}, 1000),
  ?assertEqual(ok, Ans9),
  Ans10 = receive
            {worker_answer, 5, WorkerAns5} -> WorkerAns5
          after 1000 ->
            error
          end,
  ?assertEqual(#atom{value = "ok"}, Ans10),

  Test16 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test16),
  Test17 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test17),
  Test18 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list, 1000),
  ?assertEqual([fuse1], Test18),
  Test19 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse1}, 1000),
  ?assertEqual([self()], Test19),

  Ans11 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 6, #worker_request{subject = "DN", request = #callback{fuse = fuse1, pid = self(), node = CCM, action = channelclose}}}}, 1000),
  ?assertEqual(ok, Ans11),
  Ans12 = receive
            {worker_answer, 6, WorkerAns6} -> WorkerAns6
          after 1000 ->
            error
          end,
  ?assertEqual(#atom{value = "ok"}, Ans12),

  Test20 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks, 1000),
  ?assertEqual({[], 5}, Test20),
  Test21 = gen_server:call({global, ?CCM}, get_callbacks, 1000),
  ?assertEqual({[], 5}, Test21),
  Test22 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list, 1000),
  ?assertEqual([], Test22).

validation_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,
  Port = ?config(port, Config),

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {ConAns1, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(error, ConAns1),
  {ConAns2, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_valid.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_unknown_ca.pem")}]),
  ?assertEqual(error, ConAns2),
  {ConAns3, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_outdated.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(error, ConAns3),
  {ConAns4, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_unknown_ca.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(error, ConAns4),
  {ConAns5, Socket1} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_valid.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(ok, ConAns5),
  wss:close(Socket1).

%% This test checks if client outside the cluster can ping all modules via dispatcher.
ping_test(Config) ->
  NodesUp = ?config(nodes, Config),

  %% @todo: check why dbsync sometimes does not start
  Jobs = ?MODULES -- [dbsync],
  [CCM | _] = NodesUp,
  PeerCert = ?config(peer_cert, Config),
  Port = ?config(port, Config),

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  {ConAns, Socket} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
  ?assertEqual(ok, ConAns),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

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
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  wss:close(Socket),

  ?assert(PongsNum >= length(Jobs)).

%% Tests cluster and nodes monitoring
monitoring_test(Config) ->
  Nodes = ?config(nodes, Config),
  [CCM | WorkerNodes] = Nodes,

  DuplicatedPermanentNodes = (length(WorkerNodes) - 1) * length(?PERMANENT_MODULES),

  % Init cluster
  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {ok, MonitoringInitialization} = rpc:call(CCM, application, get_env, [?APP_Name, cluster_monitoring_initialization]),
  timer:sleep(2 * 1000 * MonitoringInitialization),

  CheckStats = fun(Stats) ->
    ?assert(is_list(Stats)),
    lists:foreach(fun({_, Stat}) ->
      ?assert(is_float(Stat)),
      ?assert(Stat >= 0)
    end, Stats)
  end,

  lists:foreach(fun(Node) ->
    NodeStats = gen_server:call({?Node_Manager_Name, Node}, {get_node_stats, short}, 500),
    CheckStats(NodeStats)
  end, Nodes),

  ClusterStats = gen_server:call({global, ?CCM}, {get_cluster_stats, short}, 500),
  CheckStats(ClusterStats).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(application_start_test1, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,

  NodesUp = test_node_starter:start_test_nodes(1),
  [CCM | _] = NodesUp,

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1312}, {heart_beat, 1}]]),

  lists:append([{nodes, NodesUp}], Config);

init_per_testcase(application_start_test2, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,

  NodesUp = test_node_starter:start_test_nodes(1),
  [CCM | _] = NodesUp,

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1312}, {heart_beat, 1}]]),

  lists:append([{nodes, NodesUp}], Config);

init_per_testcase(type1, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,

  NodesUp = test_node_starter:start_test_nodes(1),
  [CCM | _] = NodesUp,

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1312}, {heart_beat, 1}]]),

  lists:append([{nodes, NodesUp}], Config);

init_per_testcase(type2, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(1),
  [CCM | _] = NodesUp,

  PeerCert = ?COMMON_FILE("peer.pem"),
  Port = 6666,
  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [CCM]}, {dns_port, 1315}, {db_nodes, [?DB_NODE]}, {heart_beat, 1}]]),

  lists:append([{port, Port}, {peer_cert, PeerCert}, {nodes, NodesUp}], Config);

init_per_testcase(monitoring_test, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = ?DB_NODE,

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [
    [{node_type, ccm}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {gateway_listener_port, 3213}, {gateway_proxy_port, 3214}, {dns_port, 1308}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {rest_port, 3308}, {db_nodes, [DBNode]}, {cluster_monitoring_initialization, 5}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {rest_port, 3309}, {db_nodes, [DBNode]}, {cluster_monitoring_initialization, 5}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {control_panel_port, 2310}, {control_panel_redirect_port, 1356}, {gateway_listener_port, 3219}, {gateway_proxy_port, 3220}, {rest_port, 3310}, {db_nodes, [DBNode]}, {cluster_monitoring_initialization, 5}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [CCM]}, {dns_port, 1311}, {control_panel_port, 2311}, {control_panel_redirect_port, 1357}, {gateway_listener_port, 3221}, {gateway_proxy_port, 3222}, {rest_port, 3311}, {db_nodes, [DBNode]}, {cluster_monitoring_initialization, 5}, {heart_beat, 1}]]),

  lists:append([{nodes, NodesUp}, {dbnode, DBNode}], Config);

init_per_testcase(TestCase, Config) ->
  case lists:member(TestCase, [modules_start_and_ping_test, workers_list_actualization_test, callbacks_list_actualization_test]) of
    true -> init_per_testcase(type1, Config);
    false -> init_per_testcase(type2, Config)
  end.

end_per_testcase(type1, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes);

end_per_testcase(type2, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node();

end_per_testcase(TestCase, Config) ->
  case lists:member(TestCase, [application_start_test1, application_start_test2, modules_start_and_ping_test, workers_list_actualization_test, callbacks_list_actualization_test, monitoring_test]) of
    true -> end_per_testcase(type1, Config);
    false -> end_per_testcase(type2, Config)
  end.
