%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how many simple messages may be sent and received
%% in standard case (open ssl socket, send/receive many messages, close socket).
%% @end
%% ===================================================================

-module(veilcluster_driver_multiple_ping).
-export([new/1, run/4]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

new(_Id) -> 
    Hosts = basho_bench_config:get(cluster_hosts),
    CertFile = basho_bench_config:get(cert_file),
    Messages_per_connect = basho_bench_config:get(messages_per_connect),
    Pong = #atom{value = "pong"},
    PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
    PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
    PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),
    Args = {Hosts, CertFile, PongAnsBytes, Messages_per_connect},
    ?DEBUG("Ping test initialized with params: ~p~n", [Args]),
    {ok, Args}.

run(Action, KeyGen, _ValueGen, {Hosts, CertFile, PongAnsBytes, Messages_per_connect}) ->
    Host = lists:nth((KeyGen() rem length(Hosts)) + 1 , Hosts),
    NewState = {Hosts, CertFile, PongAnsBytes, Messages_per_connect},
    try
      case wss:connect(Host, 5555, [{certfile, CertFile}, {cacertfile, CertFile}, auto_handshake]) of
          {ok, Socket} ->
              Res =
                  try ping(Action, Socket, PongAnsBytes, Messages_per_connect) of
                      ok -> {ok, NewState}
                  catch
                      Reason -> {error, Reason, NewState}
                  end,
            wss:close(Socket),
            Res;
          {error, Error} -> {error, {connect, Error}, NewState};
          Other -> {error, {unknown_error, Other}, NewState}
      end
    catch
      E1:E2 ->
        {error, {error_thrown, E1, E2}, NewState}
    end.

ping(Module, Socket, PongAnsBytes, Messages_per_connect) ->
    Ping = #atom{value = "ping"},
    PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

    Message = #clustermsg{module_name = atom_to_list(Module), message_type = "atom",
                            message_decoder_name = "communication_protocol", answer_type = "atom",
                            answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
    Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  multiple_ping(Messages_per_connect, Socket, Msg, PongAnsBytes).

multiple_ping(0, _, _, _) ->
  ok;

multiple_ping(Counter, Socket, Msg, PongAnsBytes) ->
  wss:send(Socket, Msg),
  Ans = case wss:recv(Socket, 0, 5000) of
    {ok, Ans1} -> Ans1;
    {error, Reason} -> throw({recv, Reason})
  end,

  case Ans of
    PongAnsBytes -> multiple_ping(Counter - 1, Socket, Msg, PongAnsBytes);
    Other -> throw({invalid_answer, Other})
  end.