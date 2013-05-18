%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of request_dispatcher.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(request_dispatcher_tests).
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test setup and teardown
%% ====================================================================

setup() ->
  net_kernel:start([node1, shortnames]),
  lager:start(),
  ssl:start(),
  ok = application:start(ranch).

teardown(_Args) ->
  ok = application:stop(ranch),
  net_kernel:stop().

%% ====================================================================
%% Test generation
%% ====================================================================

generate_test_() ->
  {setup,
    fun setup/0,
    fun teardown/1,
    [?_test(env()),
      ?_test(protocol_buffers()),
      ?_test(dispatcher_connection()),
      ?_test(workers_list_actualization()),
      ?_test(ping())
    ]}.

%% ====================================================================
%% Functions used by tests
%% ====================================================================

%% This test checks if all environment variables needed by node_manager are defined.
env() ->
  ok = application:start(?APP_Name),
  {ok, _Port} = application:get_env(veil_cluster_node, dispatcher_port),
  {ok, _PoolSize} = application:get_env(veil_cluster_node, dispatcher_pool_size),
  {ok, _RTimeout} = application:get_env(veil_cluster_node, ranch_timeout),
  {ok, _DTimeout} = application:get_env(veil_cluster_node, dispatcher_timeout),
  {ok, _Path} = application:get_env(veil_cluster_node, ssl_cert_path),
  ok = application:stop(?APP_Name).

%% This test checks dispatcher uses protocol buffer correctly.
protocol_buffers() ->
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom", answer_type = "atom",
  synch = true, protocol_version = 1, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Synch, Task, ProtocolVersion, Msg, Answer_type} = ranch_handler:decode_protocol_buffer(MessageBytes),
  ?assert(Synch),
  ?assert(Msg =:= ping),
  ?assert(Task =:= module),
  ?assert(ProtocolVersion == 1),
  ?assert(Answer_type =:= "atom"),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),

  Message2 = #answer{answer_status = "ok", worker_answer = PongBytes},
  MessageBytes2 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message2)),

  EncodedPong = ranch_handler:encode_answer(ok, "atom", pong),
  ?assert(EncodedPong =:= MessageBytes2).

%% This tests check if client may connect to dispatcher.
dispatcher_connection() ->
  Cert = "../veilfs.pem",
  application:set_env(?APP_Name, node_type, ccm),
  application:set_env(?APP_Name, ssl_cert_path, Cert),
  ok = application:start(?APP_Name),

  {ok, Port} = application:get_env(veil_cluster_node, dispatcher_port),
  {ok, Socket} = ssl:connect("localhost", Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom", answer_type = "atom",
    synch = true, protocol_version = 1, input = PingBytes},
  Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  ssl:send(Socket, Msg),
  {ok, Ans} = ssl:recv(Socket, 0),

  AnsMessage = #answer{answer_status = "wrong_worker_type"},
  AnsMessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(AnsMessage)),

  ?assert(Ans =:= AnsMessageBytes),

  Message2 = #clustermsg{module_name = "module", message_type = "atom", answer_type = "atom",
    synch = false, protocol_version = 1, input = PingBytes},
  Msg2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),
  ssl:send(Socket, Msg2),
  {ok, Ans2} = ssl:recv(Socket, 0),
  ?assert(Ans2 =:= AnsMessageBytes),

  ok = application:stop(?APP_Name).

%% This test checks if workers list inside dispatcher is refreshed correctly.
workers_list_actualization() ->
  Jobs = [cluster_rengine, control_panel, dao, fslogic, gateway, rtransfer, rule_manager],

  application:set_env(?APP_Name, node_type, ccm),
  application:set_env(?APP_Name, ccm_nodes, [node()]),
  application:set_env(?APP_Name, initialization_time, 1),

  ok = application:start(?APP_Name),
  timer:sleep(1500),

  CheckModules = fun(M, Sum) ->
    Workers = gen_server:call(?Dispatcher_Name, {get_workers, M}),
    case (length(Workers) == 1) and lists:member(node(), Workers) of
      true -> Sum + 1;
      false -> Sum
    end
  end,
  OKSum = lists:foldl(CheckModules, 0, Jobs),
  ?assert(OKSum == length(Jobs)),

  ok = application:stop(?APP_Name).

%% This test checks if client outside the cluster can ping all modules via dispatcher.
ping() ->
  Jobs = [cluster_rengine, control_panel, dao, fslogic, gateway, rtransfer, rule_manager],

  Cert = "../veilfs.pem",
  application:set_env(?APP_Name, node_type, ccm),
  application:set_env(?APP_Name, ssl_cert_path, Cert),
  application:set_env(?APP_Name, ccm_nodes, [node()]),
  application:set_env(?APP_Name, initialization_time, 1),

  ok = application:start(?APP_Name),
  timer:sleep(1500),

  {ok, Port} = application:get_env(veil_cluster_node, dispatcher_port),
  {ok, Socket} = ssl:connect("localhost", Port, [binary, {active, false}, {packet, 4}, {certfile, Cert}]),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

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
  ?assert(PongsNum == length(Jobs)),

  ok = application:stop(?APP_Name).

-endif.
