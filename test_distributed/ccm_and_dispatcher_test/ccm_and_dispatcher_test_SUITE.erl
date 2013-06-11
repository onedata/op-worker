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
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([modules_start_and_ping_test/1, dispatcher_connection_test/1, workers_list_actualization_test/1, ping_test/1, application_start_test/1]).

-include("env_setter.hrl").
-include("registered_names.hrl").
-include("records.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").

all() -> [modules_start_and_ping_test, dispatcher_connection_test, workers_list_actualization_test, ping_test, application_start_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% This function tests if application starts properly
application_start_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),

  env_setter:start_app([{node_type, ccm}, {dispatcher_port, 6666}, {ccm_nodes, [node()]}]),
  {ok, ccm} = application:get_env(?APP_Name, node_type),
  Check1 = (undefined == whereis(?Supervisor_Name)),
  Check1 = false,
  env_setter:stop_app(),

  env_setter:start_app([{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [node()]}]),
  {ok, worker} = application:get_env(?APP_Name, node_type),
  Check2 = (undefined == whereis(?Supervisor_Name)),
  Check2 = false,
  env_setter:stop_app(),

  env_setter:stop_test().

%% This function tests if ccm is able to start and connect (using gen_server messages) workers
modules_start_and_ping_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm_test}, {dispatcher_port, 6666}, {ccm_nodes, [node()]}]),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  StateNum0 = gen_server:call({global, ?CCM}, get_state_num),
  Check1 = (StateNum0 == 1),
  Check1 = true,

  gen_server:cast({global, ?CCM}, get_state_from_db),
  timer:sleep(100),
  State = gen_server:call({global, ?CCM}, get_state),
  Workers = State#cm_state.workers,
  Check2 = (length(Workers) == 1),
  Check2 = true,
  StateNum1 = gen_server:call({global, ?CCM}, get_state_num),
  Check3 = (StateNum1 == 2),
  Check3 = true,

  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(100),
  State2 = gen_server:call({global, ?CCM}, get_state),
  Workers2 = State2#cm_state.workers,
  Jobs = ?Modules,
  Check4 = (length(Workers2) == length(Jobs)),
  Check4 = true,
  StateNum2 = gen_server:call({global, ?CCM}, get_state_num),
  Check5 = (StateNum2 == 3),
  Check5 = true,

  ProtocolVersion = 1,
  CheckModules = fun(M, Sum) ->
    Ans = gen_server:call(M, {test_call, ProtocolVersion, ping}),
    case Ans of
      pong -> Sum + 1;
      _Other -> Sum
    end
  end,
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  Check6 = (PongsNum == length(Jobs)),
  Check6 = true,

  env_setter:stop_app(),
  env_setter:stop_test().

%% This tests check if client may connect to dispatcher.
dispatcher_connection_test(_Config) ->
  ?INIT_DIST_TEST,

  Cert = '../../../veilfs.pem',
  CertString = atom_to_list(Cert),
  Port = 6666,
  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [node()]}, {ssl_cert_path, Cert}]),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(100),

  {ok, Socket} = ssl:connect("localhost", Port, [binary, {active, false}, {packet, 4}, {certfile, CertString}]),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom", answer_type = "atom",
    synch = true, protocol_version = 1, input = PingBytes},
  Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  ssl:send(Socket, Msg),
  {ok, Ans} = ssl:recv(Socket, 0),

  AnsMessage = #answer{answer_status = "wrong_worker_type"},
  AnsMessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(AnsMessage)),

  Check1 = (Ans =:= AnsMessageBytes),
  Check1 = true,

  Message2 = #clustermsg{module_name = "module", message_type = "atom", answer_type = "atom",
    synch = false, protocol_version = 1, input = PingBytes},
  Msg2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),
  ssl:send(Socket, Msg2),
  {ok, Ans2} = ssl:recv(Socket, 0),
  Check2 = (Ans2 =:= AnsMessageBytes),
  Check2 = true,

  env_setter:stop_app(),
  env_setter:stop_test().

%% This test checks if workers list inside dispatcher is refreshed correctly.
workers_list_actualization_test(_Config) ->
  ?INIT_DIST_TEST,
  Cert = '../../../veilfs.pem',
  Port = 6666,
  Jobs = ?Modules,

  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [node()]}, {ssl_cert_path, Cert}]),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(100),

  CheckModules = fun(M, Sum) ->
    Workers = gen_server:call(?Dispatcher_Name, {get_workers, M}),
    case (length(Workers) == 1) of %% and lists:member(node(), Workers) of
      true -> Sum + 1;
      false -> Sum
    end
  end,
  OKSum = lists:foldl(CheckModules, 0, Jobs),
  Check1 = (OKSum == length(Jobs)),
  Check1 = true,

  env_setter:stop_app(),
  env_setter:stop_test().

%% This test checks if client outside the cluster can ping all modules via dispatcher.
ping_test(_Config) ->
  ?INIT_DIST_TEST,
  Cert = '../../../veilfs.pem',
  CertString = atom_to_list(Cert),
  Port = 6666,
  Jobs = ?Modules,

  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [node()]}, {ssl_cert_path, Cert}]),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  timer:sleep(100),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(100),

  {ok, Socket} = ssl:connect("localhost", Port, [binary, {active, false}, {packet, 4}, {certfile, CertString}]),

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
  Check1 = (PongsNum == length(Jobs)),
  Check1 = true,

  env_setter:stop_app(),
  env_setter:stop_test().