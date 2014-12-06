%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests cluster by sending large number of requests
%% to each worker. To use it enter .eunit directory (after 'make test'
%% command) and run erlang with the same cookie as oneprovider
%% uses (cookie is used to test if all modules work well after the
%% stress test). Test methods use list of pairs
%% {erlang_node, dispatcher_port}. E.g. to start test of cluster with
%% 2 nodes on localhost that use dispatcher ports 5555 and 6666 and
%% cookie 172.16.67.140 use commands:
%% cd .eunit
%% erl -name tester -setcookie 172.16.67.140 -pa ../ebin ../deps/*/ebin
%% and then in erlang shell:
%% net_adm:ping('ccm1@127.0.0.1').
%% cluster_tester:test_cluster([{'w1@127.0.0.1', 6666}, {'ccm1@127.0.0.1', 5555}]).
%% @end
%% ===================================================================
-module(cluster_tester).

-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").

%% API
-export([test_cluster/1, test_cluster/2, manytesters_node/2, manytesters_node/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% Two functions below test cluster using many tester nodes
manytesters_node(TesNodes, ClusterNodes) ->
  manytesters_node(TesNodes, ClusterNodes, 500).

manytesters_node(TesNodes, ClusterNodes, PingsNum) ->
  AnsPid = self(),
  RunTest = fun(Node) ->
    spawn(fun() ->
      Ans = rpc:call(Node, ?MODULE, test_cluster, [ClusterNodes, PingsNum]),
      AnsPid ! Ans
    end)
  end,
  lists:foreach(RunTest, TesNodes),
  {PongsNum, Time, FinalAns} = waitForAns2(length(TesNodes)),
  io:format("Final test with many testers result: ~s~n", [FinalAns]),
  io:format("Test with many testers: time in mocroseconds: ~b, pongs num: ~b, pongs per sec: ~f~n", [Time, PongsNum, PongsNum/Time*1000000]),
  {PongsNum, Time, FinalAns}.

%% Two functions below test cluster using one tester node
test_cluster(Nodes) ->
  test_cluster(Nodes, 500).

test_cluster(Nodes, PingsNum) ->
  ssl:start(),
  Ans = test_ccm(Nodes),
  {PongsNum, Time, Ans2} = ping_test(Nodes, PingsNum),
  Ans3 = test_ccm(Nodes),

  FinalAns = (Ans and Ans2 and Ans3),
  io:format("Final test result: ~s~n", [FinalAns]),
  io:format("Test time in mocroseconds: ~b, pongs num: ~b, pongs per sec: ~f~n", [Time, PongsNum, PongsNum/Time*1000000]),
  {PongsNum, Time, FinalAns}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% This function checks if state of cluster is ok
test_ccm(Nodes) ->
  Jobs = ?MODULES,

  Ans = try
    NodesFromCCM = gen_server:call({global, ?CCM}, get_nodes),
    io:format("Nodes known to ccm: ~b, nodes declared: ~b~n", [length(NodesFromCCM), length(Nodes)]),
    (length(NodesFromCCM) == length(Nodes))
  catch
    _:_ ->
    io:format("Cannot find ccm~n"),
    false
  end,

  try
    {Workers, _StateNum} = gen_server:call({global, ?CCM}, get_workers),
    io:format("Workers known to ccm: ~b, modules of system: ~b~n", [length(Workers), length(Jobs)]),
    lists:foreach(fun({Node, Module}) ->
      io:format("Module: ~s, runs on node: ~s~n", [Module, Node])
    end, Workers),

    Ans and (length(Workers) == length(Jobs))
  catch
    _:_ ->
      io:format("Cannot find ccm~n"),
      false
  end.

%% This function ping all nodes in cluster
ping_test(Nodes, PingsNum) ->
  Cert = '../onedata.pem',
  CertString = atom_to_list(Cert),
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

  Jobs = ?MODULES,
  CreateMessages = fun(M, Sum) ->
    Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom",
    message_decoder_name = "communication_protocol", answer_type = "atom",
    answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
    Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),
    [Msg | Sum]
  end,
  Messages = lists:foldl(CreateMessages, [], Jobs),

  AnsPid = self(),
  CheckNodes = fun({Node, Port}) ->
    spawn(fun() ->
      for(1, PingsNum,
        fun() -> spawn(fun() ->
          try
            NodeStr = atom_to_list(Node),
            Machine = string:sub_string(NodeStr, string:str(NodeStr, "@")+1),
            {ok, Socket} = ssl:connect(Machine, Port, [binary, {active, false}, {packet, 4}, {certfile, CertString}]),

            CheckModules = fun(M, Sum) ->
              ssl:send(Socket, M),
              {ok, Ans} = ssl:recv(Socket, 0, 5000),
              case Ans =:= PongAnsBytes of
                true -> Sum + 1;
                false -> Sum
              end
            end,

            PongsNum = lists:foldl(CheckModules, 0, Messages),
            AnsPid ! PongsNum
          catch
            _:_ ->
              io:format("Cannot connect to node: ~s using port: ~b~n", [Node, Port]),
              AnsPid ! 0
          end
        end)
      end)
    end)
  end,

  {Megaseconds,Seconds,Microseconds} = erlang:now(),
  lists:foreach(CheckNodes, Nodes),
  PongsNum2 = waitForAns(length(Nodes) * PingsNum),
  {Megaseconds2,Seconds2,Microseconds2} = erlang:now(),
  Time = 1000000*1000000*(Megaseconds2-Megaseconds) + 1000000*(Seconds2-Seconds) + Microseconds2-Microseconds,
  ExpectedPongsNum = (length(Jobs) * length(Nodes)) * PingsNum,
  io:format("Pongs number: ~b, expected: ~b~n", [PongsNum2, ExpectedPongsNum]),
  {PongsNum2, Time, PongsNum2 == ExpectedPongsNum}.

%% Internal function used as for loop
for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

%% This function waits for ProcNum integer messages and sum values of this messages
waitForAns(ProcNum) ->
  waitForAns(ProcNum, 0).
waitForAns(0, Ans) ->
  Ans;
waitForAns(ProcNum, Ans) ->
  receive
    Num -> waitForAns(ProcNum-1, Ans + Num)
  after
    5000 -> Ans
  end.

%% This function waits for ProcNum test's results and returns result of distributed test
waitForAns2(ProcNum) ->
  waitForAns2(ProcNum, {0, 0, true}).
waitForAns2(0, Ans) ->
  Ans;
waitForAns2(ProcNum, {PongsNum, Time, FinalAns}) ->
  receive
    {PongsNum2, Time2, FinalAns2} ->
      waitForAns2(ProcNum-1, {PongsNum + PongsNum2, erlang:max(Time, Time2), FinalAns and FinalAns2})
  after
    10000 -> {PongsNum, Time, FinalAns}
  end.
