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

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([main_test/1]).

all() -> [main_test].

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
    {ConAns, Socket} = ssl:connect('localhost', Port, [binary, {active, false}, {packet, 4}, {certfile, PeerCert}]),
    ?assertEqual(ok, ConAns),

    CheckModules = fun(M, Sum) ->
      Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom",
      message_decoder_name = "communication_protocol", answer_type = "atom",
      answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
      Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

      ssl:send(Socket, Msg),
      {RecvAns, Ans} = ssl:recv(Socket, 0, 5000),
      ?assertEqual(ok, RecvAns),
      case Ans =:= PongAnsBytes of
        true -> Sum + 1;
        false -> Sum
      end
    end,

    TmpPongsNum = lists:foldl(CheckModules, 0, Jobs),
    S + TmpPongsNum
  end,

  {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(length(Workers), length(Jobs)),
  PongsNum = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum, length(Jobs) * length(Ports)),

  nodes_manager:stop_node(CCM),
  timer:sleep(7000),
  ?assertEqual(CCM2, gen_server:call({global, ?CCM}, get_ccm_node)),

  {Workers2, _StateNum2} = gen_server:call({global, ?CCM}, get_workers),
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

  {Workers3, _StateNum3} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(length(Workers3), length(Jobs)),
  PongsNum3 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum3, length(Jobs) * length(Ports)),

  nodes_manager:stop_node(Worker1),
  timer:sleep(2000),

  Ports2 = [8888],
  {Workers4, _StateNum4} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(length(Workers4), length(Jobs)),
  PongsNum4 = lists:foldl(CheckNodes, 0, Ports2),
  ?assertEqual(PongsNum4, length(Jobs) * length(Ports2)),

  {StartAns2, NewNode2} = nodes_manager:start_node(Worker1, WorkerParams),
  ?assertEqual(ok, StartAns2),
  ?assertEqual(Worker1, NewNode2),
  StartLog2 = nodes_manager:start_app_on_nodes([Worker1], [WorkerArgs]),
  ?assertEqual(false, lists:member(error, StartLog2)),
  timer:sleep(2000),

  {Workers5, _StateNum5} = gen_server:call({global, ?CCM}, get_workers),
  ?assertEqual(length(Workers5), length(Jobs)),
  PongsNum5 = lists:foldl(CheckNodes, 0, Ports),
  ?assertEqual(PongsNum5, length(Jobs) * length(Ports)).


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
