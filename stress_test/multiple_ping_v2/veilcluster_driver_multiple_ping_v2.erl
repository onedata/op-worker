%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how test framework affects tests' results (when
%% we compare results to multiple_ping results).
%% @end
%% ===================================================================

-module(veilcluster_driver_multiple_ping_v2).
-export([new/1, run/4]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

new(_Id) -> 
    Hosts = basho_bench_config:get(cluster_hosts),
    CertFile = basho_bench_config:get(cert_file),
    ssl:start(),
    Pong = #atom{value = "pong"},
    PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
    PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
    PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),
    Args = {Hosts, CertFile, PongAnsBytes, closed, []},
    ?DEBUG("Ping test initialized with params: ~p~n", [Args]),
    {ok, Args}.

run(Action, KeyGen, _ValueGen, {Hosts, CertFile, PongAnsBytes, SocketState, Socket}) ->
    Host = lists:nth((KeyGen() rem length(Hosts)) + 1 , Hosts),

    {NewSocketState, NewSocket} = case SocketState of
      ok -> {SocketState, Socket};
      _ -> ssl:connect(Host, 5555, [binary, {active, false}, {packet, 4}, {certfile, CertFile}, {keyfile, CertFile}, {cacertfile, CertFile}, {reuse_sessions, false}], 5000)
    end,

%%     NewState = {Hosts, CertFile, PongAnsBytes, SocketState, NewSocket},
    case {NewSocketState, NewSocket} of
        {ok, _} ->
                try ping(Action, NewSocket, PongAnsBytes) of
                    ok -> {ok, {Hosts, CertFile, PongAnsBytes, NewSocketState, NewSocket}}
                catch
                    Reason ->
                      ssl:close(NewSocket),
                      {error, Reason, {Hosts, CertFile, PongAnsBytes, closed, []}}
                end;
        {error, Error} -> {error, {connect, Error}, {Hosts, CertFile, PongAnsBytes, closed, []}};
        Other -> {error, {unknown_error, Other}, {Hosts, CertFile, PongAnsBytes, closed, []}}
    end.

ping(Module, Socket, PongAnsBytes) ->
    Ping = #atom{value = "ping"},
    PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

    Message = #clustermsg{module_name = atom_to_list(Module), message_type = "atom",
                            message_decoder_name = "communication_protocol", answer_type = "atom",
                            answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
    Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

    ssl:send(Socket, Msg),
    Ans =
        case ssl:recv(Socket, 0, 5000) of 
            {ok, Ans1} -> Ans1;
            {error, Reason} -> throw({recv, Reason})
        end,

    case Ans of 
        PongAnsBytes -> ok;
        Other -> throw({invalid_answer, Other})
    end.