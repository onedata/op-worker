%% Copyright
-module(cluster_tester).
-author("michal").

-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").

%% API
-export([test_cluster/1, test_cluster/2]).

test_cluster(Nodes) ->
  test_cluster(Nodes, 1000).

test_cluster(Nodes, PingsNum) ->
  ssl:start(),
  Ans = test_ccm(Nodes),
  Ans2 = ping_test(Nodes, PingsNum),
  Ans3 = test_ccm(Nodes),
  FinalAns = (Ans and Ans2 and Ans3),
  io:format("Final test result: ~s~n", [FinalAns]),
  FinalAns.

test_ccm(Nodes) ->
  Jobs = ?Modules,

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

ping_test(Nodes, _PingsNum) ->
  Cert = '../veilfs.pem',
  CertString = atom_to_list(Cert),
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

  Jobs = ?Modules,
  CreateMessages = fun(M, Sum) ->
    Message = #clustermsg{module_name = atom_to_binary(M, utf8), message_type = "atom", answer_type = "atom",
    synch = true, protocol_version = 1, input = PingBytes},
    Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),
    [Msg | Sum]
  end,
  Messages = lists:foldl(CreateMessages, [], Jobs),

  CheckNodes = fun({Node, Port}, S) ->
    try
      NodeStr = atom_to_list(Node),
      Machine = string:sub_string(NodeStr, string:str(NodeStr, "@")+1),
      {ok, Socket} = ssl:connect(Machine, Port, [binary, {active, false}, {packet, 4}, {certfile, CertString}]),

      CheckModules = fun(M, Sum) ->
        ssl:send(Socket, M),
        {ok, Ans} = ssl:recv(Socket, 0),
        case Ans =:= PongAnsBytes of
          true -> Sum + 1;
          false -> Sum
        end
      end,

      PongsNum = lists:foldl(CheckModules, 0, Messages),
      S + PongsNum
    catch
      _:_ ->
        io:format("Cannot connect to node: ~s using port: ~b~n", [Node, Port]),
        S
    end
  end,

  PongsNum2 = lists:foldl(CheckNodes, 0, Nodes),
  ExpectedPongsNum = (length(Jobs) * length(Nodes)),
  io:format("Pongs number: ~b, expected: ~b~n", [PongsNum2, ExpectedPongsNum]),
  PongsNum2 == ExpectedPongsNum.