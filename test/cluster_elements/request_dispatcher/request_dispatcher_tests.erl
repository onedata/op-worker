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

env() ->
  ok = application:start(?APP_Name),
  {ok, _Port} = application:get_env(veil_cluster_node, dispatcher_port),
  {ok, _PoolSize} = application:get_env(veil_cluster_node, dispatcher_pool_size),
  ok = application:stop(?APP_Name).

protocol_buffers() ->
  Ping = #atom{atom = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom", input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Mod, Msg} = ranch_handler:decode_protocol_buffer(MessageBytes),
  ?assert(Mod =:= module),
  ?assert(Msg =:= ping).

dispatcher_connection() ->
  application:set_env(?APP_Name, node_type, ccm),
  ok = application:start(?APP_Name),

  {ok, Port} = application:get_env(veil_cluster_node, dispatcher_port),
  {ok, Socket} = gen_tcp:connect("localhost", Port, [binary,{active, false}]),
  Msg = {synch, not_existing_module, 1, "message"},
  gen_tcp:send(Socket, term_to_binary(Msg)),
  {ok, Ans} = gen_tcp:recv(Socket, 0),
  ?assert(Ans =:= <<"wrong_worker_type">>),

  Msg2 = {asynch, not_existing_module, 1, "message"},
  gen_tcp:send(Socket, term_to_binary(Msg2)),
  {ok, Ans2} = gen_tcp:recv(Socket, 0),
  ?assert(Ans2 =:= <<"wrong_worker_type">>),

  ok = application:stop(?APP_Name).

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

ping() ->
  Jobs = [cluster_rengine, control_panel, dao, fslogic, gateway, rtransfer, rule_manager],

  application:set_env(?APP_Name, node_type, ccm),
  application:set_env(?APP_Name, ccm_nodes, [node()]),
  application:set_env(?APP_Name, initialization_time, 1),

  ok = application:start(?APP_Name),
  timer:sleep(1500),

  {ok, Port} = application:get_env(veil_cluster_node, dispatcher_port),
  {ok, Socket} = gen_tcp:connect("localhost", Port, [binary,{active, false}]),
  ProtocolVersion = 1,
  CheckModules = fun(M, Sum) ->
    Msg = {synch, M, 1, ping},
    gen_tcp:send(Socket, term_to_binary(Msg)),
    {ok, Ans} = gen_tcp:recv(Socket, 0),
    case binary_to_atom(Ans,utf8) of
      pong -> Sum + 1;
      _Other -> Sum
    end
  end,
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  ?assert(PongsNum == length(Jobs)),

  ok = application:stop(?APP_Name).

-endif.
