%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how many simple messages may be sent and received.
%% It uses very fast Erlang ping but it additionally codes protocol buffer
%% message before each operation. This test allows to check if protocol buffer
%% may be a performance problem (when we compare results to limit test results).
%% @end
%% ===================================================================

-module(veilcluster_driver_limit_with_pb).
-export([new/1, run/4]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

new(Id) ->
  ?DEBUG("limitlog2: test started with id: ~p~n", [Id]),
  Hosts = basho_bench_config:get(cluster_erlang_nodes),
  try
    ?DEBUG("limitlog2: hosts: ~p~n", [Hosts]),
    H1 = os:cmd("hostname"),
    H2 = string:substr(H1, 1, length(H1) - 1),
    ?DEBUG("limitlog2: host name: ~p~n", [H2]),
    Ans1 = net_kernel:start([list_to_atom("tester@" ++ H2), longnames]),
    ?DEBUG("limitlog2: net_kernel ans: ~p~n", [Ans1]),
    Ans2 = erlang:set_cookie(node(), veil_cluster_node),
    ?DEBUG("limitlog2: set_cookie ans: ~p~n", [Ans2])
  catch
    E1:E2 -> ?DEBUG("limitlog2: init error: ~p:~p~n", [E1, E2])
  end,

  CertFile = basho_bench_config:get(cert_file),
  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
  PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
  PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),
  Args = {Hosts, CertFile, PongAnsBytes},
  ?DEBUG("limitlog2: Ping test initialized with params: ~p~n", [Args]),
  {ok, Args}.

run(Action, KeyGen, _ValueGen, {Hosts, CertFile, PongAnsBytes}) ->
  Host = lists:nth((KeyGen() rem length(Hosts)) + 1 , Hosts),
  NewState = {Hosts, CertFile, PongAnsBytes},
        try ping(Action, Host, PongAnsBytes) of
          ok -> {ok, NewState}
        catch
          Reason -> {error, Reason, NewState}
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