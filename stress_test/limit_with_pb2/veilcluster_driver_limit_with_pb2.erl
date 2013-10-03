%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how many simple messages may be sent and received.
%% It uses very fast Erlang ping but it additionally codes protocol buffer
%% message and opens ssl socket before each operation. This test allows to
%% check if protocol buffer or socket opening may be a performance problem
%% (when we compare results to limit test results).
%% @end
%% ===================================================================

-module(veilcluster_driver_limit_with_pb2).
-export([new/1, run/4]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

new(Id) ->
  ?DEBUG("limitlog3: test started with id: ~p~n", [Id]),
  Hosts = basho_bench_config:get(cluster_erlang_nodes),
  try
    ?DEBUG("limitlog3: hosts: ~p~n", [Hosts]),
    H1 = os:cmd("hostname"),
    H2 = string:substr(H1, 1, length(H1) - 1),
    ?DEBUG("limitlog3: host name: ~p~n", [H2]),
    Ans1 = net_kernel:start([list_to_atom("tester@" ++ H2), longnames]),
    ?DEBUG("limitlog3: net_kernel ans: ~p~n", [Ans1]),
    Ans2 = erlang:set_cookie(node(), veil_cluster_node),
    ?DEBUG("limitlog3: set_cookie ans: ~p~n", [Ans2])
  catch
    E1:E2 -> ?DEBUG("limitlog3: init error: ~p:~p~n", [E1, E2])
  end,

  Hosts2 = basho_bench_config:get(cluster_hosts),
  CertFile = basho_bench_config:get(cert_file),
  ssl:start(),
  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),
  Args = {Hosts, Hosts2, CertFile, PongAnsBytes},
  ?DEBUG("limitlog3: Ping test initialized with params: ~p~n", [Args]),
  {ok, Args}.

run(Action, KeyGen, _ValueGen, {Hosts, Hosts2, CertFile, PongAnsBytes}) ->
  KG = KeyGen(),
  Host = lists:nth((KG rem length(Hosts)) + 1 , Hosts),
  Host2 = lists:nth((KG rem length(Hosts2)) + 1 , Hosts2),
  NewState = {Hosts, Hosts2, CertFile, PongAnsBytes},
  case ssl:connect(Host2, 5555, [binary, {active, false}, {packet, 4}, {certfile, CertFile}, {keyfile, CertFile}, {cacertfile, CertFile}, {reuse_sessions, false}], 5000) of
    {ok, Socket} ->
      Res =
        try ping(Action, Host, PongAnsBytes) of
          ok -> {ok, NewState}
        catch
          Reason -> {error, Reason, NewState}
        end,
      ssl:close(Socket),
      Res;
    {error, Error} -> {error, {connect, Error}, NewState};
    Other -> {error, {unknown_error, Other}, NewState}
  end.

ping(Module, Host, _PongAnsBytes) ->
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = atom_to_list(Module), message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
  _Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  case net_adm:ping(Host) of
    pong -> ok;
    Other -> throw({invalid_answer, Other})
  end.