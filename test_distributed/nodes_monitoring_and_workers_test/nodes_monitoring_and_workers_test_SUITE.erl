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

-module(nodes_monitoring_and_workers_test_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("env_setter.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").

-export([all/0]).
-export([ccm1_test/1, worker1_test/1, worker2_test/1, worker3_test/1, tester_test/1]).

all() -> [ccm1_test, ccm2_test, worker_test, tester_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% This function runs on node that hosts ccm
ccm1_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:synch_nodes(['worker1@localhost', 'worker2@localhost', 'worker3@localhost', 'tester@localhost']),

  Cert = '../../../veilfs.pem',
  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [node()]}, {ssl_cert_path, Cert}, {dns_port, 1308}]),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(2000),
  gen_server:cast({global, ?CCM}, init_cluster),

  timer:sleep(3000),
  env_setter:stop_app(),
  env_setter:stop_test().

%% This function runs on node that hosts worker1
worker1_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:synch_nodes(['ccm1@localhost', 'worker2@localhost', 'worker3@localhost', 'tester@localhost']),

  Cert = '../../../veilfs.pem',
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, ['ccm1@localhost']}, {ssl_cert_path, Cert}, {dns_port, 1309}]),
  timer:sleep(1000),
  gen_server:cast(?Node_Manager_Name, do_heart_beat),

  timer:sleep(4000),
  env_setter:stop_app(),
  env_setter:stop_test().

%% This function runs on node that hosts worker2
worker2_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:synch_nodes(['ccm1@localhost', 'worker1@localhost', 'worker3@localhost', 'tester@localhost']),

  Cert = '../../../veilfs.pem',
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, ['ccm1@localhost']}, {ssl_cert_path, Cert}, {dns_port, 1310}]),
  timer:sleep(1000),
  gen_server:cast(?Node_Manager_Name, do_heart_beat),

  timer:sleep(4000),
  env_setter:stop_app(),
  env_setter:stop_test().

%% This function runs on node that hosts worker3
worker3_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:synch_nodes(['ccm1@localhost', 'worker1@localhost', 'worker2@localhost', 'tester@localhost']),

  Cert = '../../../veilfs.pem',
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, ['ccm1@localhost']}, {ssl_cert_path, Cert}, {dns_port, 1311}]),
  timer:sleep(1000),
  gen_server:cast(?Node_Manager_Name, do_heart_beat),

  timer:sleep(4000),
  env_setter:stop_app(),
  env_setter:stop_test().

%% This function connects with other nodes using ssl and checks if cluster works properly
tester_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:synch_nodes(['ccm1@localhost', 'worker1@localhost', 'worker2@localhost', 'worker3@localhost']),

  env_setter:start_test(),
  global:sync(),
  timer:sleep(3000),
  NotExistingNodes = ['n1@localhost', 'n2@localhost', 'n3@localhost'],
  lists:foreach(fun(Node) -> gen_server:call({global, ?CCM}, {node_is_up, Node}) end, NotExistingNodes),

  NodesUp = ['ccm1@localhost', 'worker1@localhost', 'worker2@localhost', 'worker3@localhost'],
  Nodes = gen_server:call({global, ?CCM}, get_nodes),
  Check1 = (length(Nodes) == length(NodesUp)),
    try
    Check1 = (length(Nodes) < 1000)
      catch
        _:_ -> ok
  end,
  Check1 = true,
    lists:foreach(fun(Node) ->
      Check2 = (lists:member(Node, Nodes)),
      Check2 = true
    end, NodesUp),

  lists:foreach(fun(Node) -> gen_server:call({global, ?CCM}, {node_is_up, Node}) end, NodesUp),
  Nodes2 = gen_server:call({global, ?CCM}, get_nodes),
  Check3 = (length(Nodes2) == length(NodesUp)),
  Check3 = true,

  {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers),
  Jobs = ?Modules,
  Check4 = (length(Workers) == length(Jobs)),
  Check4 = true,

  Cert = '../../../veilfs.pem',
  CertString = atom_to_list(Cert),
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

  Ports = [5055, 6666, 7777, 8888],
  CheckNodes = fun(Port, S) ->
    {ok, Socket} = ssl:connect('localhost', Port, [binary, {active, false}, {packet, 4}, {certfile, CertString}]),

    CheckModules = fun(M, Sum) ->
      Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom",
      message_decoder_name = "communication_protocol", answer_type = "atom",
      answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
      Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

      ssl:send(Socket, Msg),
      {ok, Ans} = ssl:recv(Socket, 0),
      case Ans =:= PongAnsBytes of
        true -> Sum + 1;
        false -> Sum
      end
    end,

    PongsNum = lists:foldl(CheckModules, 0, Jobs),
    S + PongsNum
  end,

  PongsNum2 = lists:foldl(CheckNodes, 0, Ports),
  Check5 = (PongsNum2 == (length(Jobs) * length(Ports))),
  Check5 = true,

  env_setter:stop_test().