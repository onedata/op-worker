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
-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").

%% export for ct
-export([all/0]).
-export([main_test/1]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [main_test].

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

main_test(_Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),
  NodesUp = nodes_manager:start_test_on_nodes(4),
  false = lists:member(error, NodesUp),

  [CCM | WorkerNodes] = NodesUp,

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1309}],
    [{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, [CCM]}, {dns_port, 1310}],
    [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [CCM]}, {dns_port, 1311}]]),
  false = lists:member(error, StartLog),

  ok = rpc:call(CCM, ?MODULE, ccm_code1, []),
  timer:sleep(100),
  RunWorkerCode = fun(Node) ->
    ok = rpc:call(Node, ?MODULE, worker_code, [])
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  timer:sleep(100),
  ok = rpc:call(CCM, ?MODULE, ccm_code2, []),

  NotExistingNodes = ['n1@localhost', 'n2@localhost', 'n3@localhost'],
  lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NotExistingNodes),
  timer:sleep(100),
  Nodes = gen_server:call({global, ?CCM}, get_nodes),
  Check1 = (length(Nodes) == length(NodesUp)),
  Check1 = true,
    lists:foreach(fun(Node) ->
      Check2 = (lists:member(Node, Nodes)),
      Check2 = true
    end, NodesUp),

  lists:foreach(fun(Node) -> gen_server:cast({global, ?CCM}, {node_is_up, Node}) end, NodesUp),
  timer:sleep(100),
  Nodes2 = gen_server:call({global, ?CCM}, get_nodes),
  Check3 = (length(Nodes2) == length(NodesUp)),
  Check3 = true,

  {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers),
  Jobs = ?Modules,
  Check4 = (length(Workers) == length(Jobs)),
  Check4 = true,

  PeerCert = ?COMMON_FILE("peer.pem"),
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

  Ports = [5055, 6666, 7777, 8888],
  CheckNodes = fun(Port, S) ->
    {ok, Socket} = ssl:connect('localhost', Port, [binary, {active, false}, {packet, 4}, {certfile, PeerCert}]),

    CheckModules = fun(M, Sum) ->
      Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom",
      message_decoder_name = "communication_protocol", answer_type = "atom",
      answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
      Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

      ssl:send(Socket, Msg),
      {ok, Ans} = ssl:recv(Socket, 0, 5000),
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

  StopLog = nodes_manager:stop_app_on_nodes(NodesUp),
  false = lists:member(error, StopLog),
  ok = nodes_manager:stop_nodes(NodesUp),
  nodes_manager:stop_deps_for_tester_node().
