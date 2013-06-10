-module(nodes_monitoring_and_workers_test_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("env_setter.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").

-export([all/0]).
-export([ccm1_test/1, worker1_test/1, worker2_test/1, worker3_test/1, tester_test/1]).

all() -> [ccm1_test, ccm2_test, worker_test, tester_test].

ccm1_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm}, {dispatcher_port, 5055}, {heart_beat, 1}, {initialization_time, 5}, {ccm_nodes, ['ccm1@localhost']}]),
  timer:sleep(10000),
  env_setter:stop_app(),
  env_setter:stop_test().

worker1_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 6666}, {heart_beat, 1}, {ccm_nodes, ['ccm1@localhost']}]),
  timer:sleep(10000),
  env_setter:stop_app(),
  env_setter:stop_test().

worker2_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 7777}, {heart_beat, 1}, {ccm_nodes, ['ccm1@localhost']}]),
  timer:sleep(10000),
  env_setter:stop_app(),
  env_setter:stop_test().

worker3_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 8888}, {heart_beat, 1}, {ccm_nodes, ['ccm1@localhost']}]),
  timer:sleep(10000),
  env_setter:stop_app(),
  env_setter:stop_test().

tester_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  timer:sleep(500),
  pong = net_adm:ping('ccm1@localhost'),

  timer:sleep(7000),
  NotExistingNodes = ['n1@localhost', 'n2@localhost', 'n3@localhost'],
  lists:foreach(fun(Node) -> gen_server:call({global, ?CCM}, {node_is_up, Node}) end, NotExistingNodes),

  NodesUp = ['ccm1@localhost', 'worker1@localhost', 'worker2@localhost', 'worker3@localhost'],
 	Nodes = gen_server:call({global, ?CCM}, get_nodes),
  Check1 = (length(Nodes) == length(NodesUp)),
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
      Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom", answer_type = "atom",
      synch = true, protocol_version = 1, input = PingBytes},
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