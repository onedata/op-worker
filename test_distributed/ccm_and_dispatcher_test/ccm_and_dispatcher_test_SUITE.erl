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
-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("records.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([modules_start_and_ping_test/1, dispatcher_connection_test/1, workers_list_actualization_test/1, ping_test/1, application_start_test1/1, application_start_test2/1, validation_test/1, callbacks_list_actualization_test/1]).

%% export nodes' codes
-export([application_start_test_code1/0, application_start_test_code2/0]).

all() -> [application_start_test1, application_start_test2, modules_start_and_ping_test, workers_list_actualization_test, validation_test, ping_test, dispatcher_connection_test, callbacks_list_actualization_test].

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
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node | _] = NodesUp,

  ?assertEqual({ok, ccm}, rpc:call(Node, ?MODULE, application_start_test_code1, [])),
  ?assert(rpc:call(Node, ?MODULE, application_start_test_code2, [])).

%% This function tests if worker application starts properly
application_start_test2(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [Node | _] = NodesUp,

  ?assertEqual({ok, worker}, rpc:call(Node, ?MODULE, application_start_test_code1, [])),
  ?assert(rpc:call(Node, ?MODULE, application_start_test_code2, [])).

%% This function tests if ccm is able to start and connect (using gen_server messages) workers
modules_start_and_ping_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  ?assertEqual(1, gen_server:call({global, ?CCM}, get_state_num)),

  gen_server:cast({global, ?CCM}, get_state_from_db),
  timer:sleep(100),
  State = gen_server:call({global, ?CCM}, get_state),
  Workers = State#cm_state.workers,
  ?assertEqual(1, length(Workers)),
  ?assertEqual(2, gen_server:call({global, ?CCM}, get_state_num)),

  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(100),
  State2 = gen_server:call({global, ?CCM}, get_state),
  Workers2 = State2#cm_state.workers,
  Jobs = ?Modules,
  ?assertEqual(length(Workers2), length(Jobs)),
  ?assertEqual(3, gen_server:call({global, ?CCM}, get_state_num)),

  ProtocolVersion = 1,
  CheckModules = fun(M, Sum) ->
    Ans = gen_server:call({M, CCM}, {test_call, ProtocolVersion, ping}),
    case Ans of
      pong -> Sum + 1;
      _Other -> Sum
    end
  end,
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  ?assertEqual(PongsNum, length(Jobs)).

%% This tests check if client may connect to dispatcher.
dispatcher_connection_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,

  PeerCert = ?config(peer_cert, Config),
  Port = ?config(port, Config),

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {ConAns, Socket} = wss:connect('localhost', Port, [{certfile, PeerCert}]),
  ?assertEqual(ok, ConAns),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom", message_decoder_name = "communication_protocol",
  answer_type = "atom", answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
  Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  wss:send(Socket, Msg),
  {RecvAns, Ans} = wss:recv(Socket, 5000),
  ?assertEqual(ok, RecvAns),

  AnsMessage = #answer{answer_status = "wrong_worker_type"},
  AnsMessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(AnsMessage)),

  ?assertEqual(Ans, AnsMessageBytes),

  Message2 = #clustermsg{module_name = "module", message_type = "atom", message_decoder_name = "communication_protocol",
  answer_type = "atom", answer_decoder_name = "communication_protocol", synch = false, protocol_version = 1, input = PingBytes},
  Msg2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),
  wss:send(Socket, Msg2),
  {RecvAns2, Ans2} = wss:recv(Socket, 5000),
  ?assertEqual(ok, RecvAns2),
  ?assertEqual(Ans2, AnsMessageBytes).

%% This test checks if workers list inside dispatcher is refreshed correctly.
workers_list_actualization_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Jobs = ?Modules,
  [CCM | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  CheckModules = fun(M, Sum) ->
    Workers = gen_server:call({?Dispatcher_Name, CCM}, {get_workers, M}),
    case (length(Workers) == 1) of %% and lists:member(node(), Workers) of
      true -> Sum + 1;
      false -> Sum
    end
  end,
  OKSum = lists:foldl(CheckModules, 0, Jobs),
  ?assertEqual(OKSum, length(Jobs)).

%% This test checks if callbacks list inside dispatcher is refreshed correctly.
callbacks_list_actualization_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  Ans1 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 1, #veil_request{subject = "DN", request = #callback{fuse = fuse1, pid = self(), node = CCM, action = channelregistration}}}}),
  ?assertEqual(ok, Ans1),
  Ans2 = receive
    {worker_answer, 1, WorkerAns} -> WorkerAns
  after 1000 ->
    error
  end,
  ?assertEqual(#atom{value = "ok"}, Ans2),

  Test1 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}], 2}, Test1),
  Test2 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}], 2}, Test2),
  Test3 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list),
  ?assertEqual([fuse1], Test3),
  Test4 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse1}),
  ?assertEqual([self()], Test4),

  Ans3 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 2, #veil_request{subject = "DN", request = #callback{fuse = fuse2, pid = self(), node = CCM, action = channelregistration}}}}),
  ?assertEqual(ok, Ans3),
  Ans4 = receive
           {worker_answer, 2, WorkerAns2} -> WorkerAns2
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans4),

  Test5 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test5),
  Test6 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test6),
  Test7 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list),
  ?assertEqual([fuse2, fuse1], Test7),
  Test8 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse2}),
  ?assertEqual([self()], Test8),

  Ans5 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 3, #veil_request{subject = "DN", request = #callback{fuse = fuse1, pid = pid2, node = CCM, action = channelregistration}}}}),
  ?assertEqual(ok, Ans5),
  Ans6 = receive
           {worker_answer, 3, WorkerAns3} -> WorkerAns3
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans6),

  Test9 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test9),
  Test10 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}, {fuse2, [CCM]}], 3}, Test10),
  Test11 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list),
  ?assertEqual([fuse1, fuse2], Test11),
  Test12 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse1}),
  ?assertEqual([pid2, self()], Test12),

  Ans7 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 4, #veil_request{subject = "DN", request = #callback{fuse = fuse2, pid = self(), node = CCM, action = channelclose}}}}),
  ?assertEqual(ok, Ans7),
  Ans8 = receive
           {worker_answer, 4, WorkerAns4} -> WorkerAns4
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans8),

  Test13 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test13),
  Test14 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test14),
  Test15 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list),
  ?assertEqual([fuse1], Test15),

  Ans9 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 5, #veil_request{subject = "DN", request = #callback{fuse = fuse1, pid = pid2, node = CCM, action = channelclose}}}}),
  ?assertEqual(ok, Ans9),
  Ans10 = receive
           {worker_answer, 5, WorkerAns5} -> WorkerAns5
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans10),

  Test16 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test16),
  Test17 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual({[{fuse1, [CCM]}], 4}, Test17),
  Test18 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list),
  ?assertEqual([fuse1], Test18),
  Test19 = gen_server:call({?Node_Manager_Name, CCM}, {get_all_callbacks, fuse1}),
  ?assertEqual([self()], Test19),

  Ans11 = gen_server:call({?Dispatcher_Name, CCM}, {node_chosen, {fslogic, 1, self(), 6, #veil_request{subject = "DN", request = #callback{fuse = fuse1, pid = self(), node = CCM, action = channelclose}}}}),
  ?assertEqual(ok, Ans11),
  Ans12 = receive
           {worker_answer, 6, WorkerAns6} -> WorkerAns6
         after 1000 ->
           error
         end,
  ?assertEqual(#atom{value = "ok"}, Ans12),

  Test20 = gen_server:call({?Dispatcher_Name, CCM}, get_callbacks),
  ?assertEqual({[], 5}, Test20),
  Test21 = gen_server:call({global, ?CCM}, get_callbacks),
  ?assertEqual({[], 5}, Test21),
  Test22 = gen_server:call({?Node_Manager_Name, CCM}, get_fuses_list),
  ?assertEqual([], Test22).

validation_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,
  Port = ?config(port, Config),

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

  {ConAns1, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(error, ConAns1),
  {ConAns2, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_valid.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_unknown_ca.pem")}]),
  ?assertEqual(error, ConAns2),
  {ConAns3, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_outdated.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(error, ConAns3),
  {ConAns4, _} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_unknown_ca.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(error, ConAns4),
  {ConAns5, _Socket1} = wss:connect('localhost', Port, [{certfile, ?TEST_FILE("certs/proxy_valid.pem")}, {cacertfile, ?TEST_FILE("certs/proxy_valid.pem")}]),
  ?assertEqual(ok, ConAns5).

%% This test checks if client outside the cluster can ping all modules via dispatcher.
ping_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  Jobs = ?Modules,
  [CCM | _] = NodesUp,
  PeerCert = ?config(peer_cert, Config),
  Port = ?config(port, Config),

  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(1500),

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
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  ?assertEqual(PongsNum, length(Jobs)).

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(application_start_test1, Config) ->
  ?INIT_DIST_TEST,

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [CCM | _] = NodesUp,

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1312}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}], Config);

init_per_testcase(application_start_test2, Config) ->
  ?INIT_DIST_TEST,

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [CCM | _] = NodesUp,

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1312}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}], Config);

init_per_testcase(type1, Config) ->
  ?INIT_DIST_TEST,

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [CCM | _] = NodesUp,

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1312}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}], Config);

init_per_testcase(type2, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [CCM | _] = NodesUp,

  PeerCert = ?COMMON_FILE("peer.pem"),
  Port = 6666,
  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [CCM]}, {dns_port, 1315}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{port, Port}, {peer_cert, PeerCert}, {nodes, NodesUp}, {assertions, Assertions}], Config);

init_per_testcase(TestCase, Config) ->
  case lists:member(TestCase, [modules_start_and_ping_test, workers_list_actualization_test, callbacks_list_actualization_test]) of
    true -> init_per_testcase(type1, Config);
    false -> init_per_testcase(type2, Config)
  end.

end_per_testcase(type1, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns);

end_per_testcase(type2, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns);

end_per_testcase(TestCase, Config) ->
  case lists:member(TestCase, [application_start_test1, application_start_test2, modules_start_and_ping_test, workers_list_actualization_test, callbacks_list_actualization_test]) of
    true -> end_per_testcase(type1, Config);
    false -> end_per_testcase(type2, Config)
  end.